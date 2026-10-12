package web

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/szporwolik/WarnFlux/internal/action"
	"github.com/szporwolik/WarnFlux/internal/actions/discord"
	"github.com/szporwolik/WarnFlux/internal/actions/smtp"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	"github.com/szporwolik/WarnFlux/internal/markdown"
	"github.com/szporwolik/WarnFlux/internal/storage"
)

// massSendTimeout bounds one broadcast (recipient resolution plus every
// channel loop).
const massSendTimeout = 90 * time.Second

// massJobRetention is how long a finished broadcast stays readable for the
// progress poller.
const massJobRetention = 10 * time.Minute

// massChannelOrder is the delivery order of the broadcast channels; the
// progress panel shows the selected ones in this order.
var massChannelOrder = []string{"aprs", "sms", "email", "discord", "meshtastic"}

// massChannelLabel maps a channel kind to its i18n label key.
var massChannelLabel = map[string]string{
	"aprs": "mass.ch.aprs", "sms": "mass.ch.sms", "email": "mass.ch.email",
	"discord": "mass.ch.discord", "meshtastic": "mass.ch.meshtastic",
}

// massSendRequest is the validated broadcast captured at submit time and
// handed to the background job.
type massSendRequest struct {
	kind     string // massKindShort | massKindLong
	message  string
	channels []string // selected kinds, in massChannelOrder
	userIDs  []int64
	groupIDs []int64
	slugs    []string
	username string
	lang     string
	app      action.AppInfo
}

// markdown reports whether the message is composed as markdown (the long
// kind), which decides how e-mail and Discord render it.
func (r massSendRequest) markdown() bool { return r.kind == massKindLong }

// massChannelProgress is the live progress of one delivery channel.
type massChannelProgress struct {
	kind   string
	state  string // pending | running | done | incomplete
	total  int
	done   int
	sent   int
	failed int
	// unsent counts the recipients that were never attempted because the
	// broadcast ran out of its budget: "done" would read as a complete
	// delivery (reported P2).
	unsent int
}

// massJob is one broadcast running in the background. Its mutable state is
// guarded by mu: the runner goroutine and the progress poller both touch it.
type massJob struct {
	mu         sync.Mutex
	id         string
	createdAt  time.Time
	req        massSendRequest
	resolved   bool
	recipients int
	channels   []*massChannelProgress
	finished   bool
	// incomplete records that at least one channel ran out of budget: the
	// job is finished, but not every recipient was reached.
	incomplete bool
	summary    string
}

// massJobStore tracks the running and recently finished broadcasts. Entries
// are pruned by age, so the map stays bounded.
type massJobStore struct {
	mu   sync.Mutex
	jobs map[string]*massJob
}

func newMassJobStore() *massJobStore { return &massJobStore{jobs: make(map[string]*massJob)} }

func (st *massJobStore) add(j *massJob) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for id, old := range st.jobs {
		if time.Since(old.created()) > massJobRetention {
			delete(st.jobs, id)
		}
	}
	st.jobs[j.id] = j
}

func (st *massJobStore) get(id string) *massJob {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.jobs[id]
}

func (j *massJob) created() time.Time {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.createdAt
}

// setRecipients records the resolved recipient count (the "preparing" phase
// ends here).
func (j *massJob) setRecipients(n int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.recipients = n
	j.resolved = true
}

// setChannelTotal records how many recipients a channel will attempt.
func (j *massJob) setChannelTotal(kind string, total int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if c := j.channel(kind); c != nil {
		c.total = total
		if total > 0 {
			c.state = "running"
		} else {
			c.state = "done"
		}
	}
}

// record advances a channel by one recipient.
func (j *massJob) record(kind string, ok bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.channel(kind)
	if c == nil {
		return
	}
	c.done++
	if ok {
		c.sent++
	} else {
		c.failed++
	}
}

// finishChannel closes one channel: a channel that attempted FEWER
// recipients than it announced ran out of the broadcast budget, so it is
// marked incomplete and the recipients it never reached are counted as
// unsent instead of vanishing (reported P2).
func (j *massJob) finishChannel(kind string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.channel(kind)
	if c == nil {
		return
	}
	if c.done < c.total {
		c.unsent = c.total - c.done
		c.state = "incomplete"
		j.incomplete = true
		return
	}
	c.state = "done"
}

// unsentTotal reports how many messages of the whole broadcast were never
// attempted, and which state the channels carry (for the summary).
func (j *massJob) unsentTotal() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	n := 0
	for _, c := range j.channels {
		n += c.unsent
	}
	return n
}

// finish marks the whole job complete with its final summary.
func (j *massJob) finish(summary string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.finished = true
	j.summary = summary
}

// channel returns the progress record of kind (caller holds mu).
func (j *massJob) channel(kind string) *massChannelProgress {
	for _, c := range j.channels {
		if c.kind == kind {
			return c
		}
	}
	return nil
}

// channelCounts returns (sent, failed) of one channel (for the summary).
func (j *massJob) channelCounts(kind string) (int, int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if c := j.channel(kind); c != nil {
		return c.sent, c.failed
	}
	return 0, 0
}

// massProgressChannel is one pre-rendered channel chip.
type massProgressChannel struct {
	Label  string
	State  string // pending | running | done
	Detail string
}

// massProgressView is the fully pre-rendered progress fragment (all text is
// resolved in Go, so the partial needs no template funcs or language).
type massProgressView struct {
	JobID      string
	Finished   bool
	Failed     bool
	Incomplete bool
	Percent    int
	Headline   string
	Where      string
	Summary    string
	Channels   []massProgressChannel
}

// progressView snapshots the job for rendering.
func (j *massJob) progressView(lang string) massProgressView {
	j.mu.Lock()
	defer j.mu.Unlock()

	v := massProgressView{
		JobID:    j.id,
		Finished: j.finished,
		Headline: i18n.T(lang, "mass.progress.title"),
	}
	total, done := 0, 0
	for _, c := range j.channels {
		total += c.total
		done += c.done
		label := i18n.T(lang, massChannelLabel[c.kind])
		detail := ""
		switch c.state {
		case "running":
			detail = fmt.Sprintf("%d/%d", c.done, c.total)
			if v.Where == "" {
				v.Where = fmt.Sprintf("%s · %s", label, fmt.Sprintf(i18n.T(lang, "mass.progress.user"), c.done, c.total))
			}
		case "done", "incomplete":
			detail = fmt.Sprintf("%d ✓", c.sent)
			if c.failed > 0 {
				detail += fmt.Sprintf(" · %d ✗", c.failed)
			}
			if c.unsent > 0 {
				detail += " · " + fmt.Sprintf(i18n.T(lang, "mass.progress.unsent"), c.unsent)
				v.Incomplete = true
			}
			if c.failed > 0 {
				v.Failed = true
			}
		default:
			detail = i18n.T(lang, "mass.progress.queued")
		}
		v.Channels = append(v.Channels, massProgressChannel{Label: label, State: c.state, Detail: detail})
	}
	if total > 0 {
		v.Percent = done * 100 / total
	}
	if j.finished {
		// An incomplete broadcast must NOT read as 100%: the bar shows how
		// much of the recipient list was actually reached.
		if !v.Incomplete {
			v.Percent = 100
		}
		v.Incomplete = v.Incomplete || j.incomplete
		v.Headline = i18n.T(lang, "mass.progress.done")
		if v.Incomplete {
			v.Headline = i18n.T(lang, "mass.progress.incomplete")
		}
		v.Where = ""
		v.Summary = j.summary
	} else if !j.resolved {
		v.Where = i18n.T(lang, "mass.progress.preparing")
	}
	return v
}

// startMassJob registers the job and runs it in the background.
func (s *Server) startMassJob(req massSendRequest) *massJob {
	j := &massJob{id: newMassJobID(), createdAt: time.Now(), req: req}
	for _, kind := range req.channels {
		j.channels = append(j.channels, &massChannelProgress{kind: kind, state: "pending"})
	}
	s.massJobs.add(j)
	go s.runMassJob(j)
	return j
}

// newMassJobID returns a random, URL-safe job id.
func newMassJobID() string {
	if tok, err := randomToken(); err == nil {
		return tok
	}
	return fmt.Sprintf("j%d", time.Now().UnixNano())
}

// runMassJob resolves the recipients and delivers the notice channel by
// channel, recording progress after every recipient.
func (s *Server) runMassJob(j *massJob) {
	ctx, cancel := context.WithTimeout(s.baseCtx, massSendTimeout)
	defer cancel()
	req := j.req

	recipients := s.massRecipients(ctx, req.userIDs, req.groupIDs, req.slugs)
	j.setRecipients(len(recipients))

	for _, kind := range req.channels {
		// The budget is spent: the channel is entered anyway — it stops
		// at once (the workers check the context) but records its
		// recipients, so the ones it never reached are counted as unsent
		// instead of disappearing from the progress and the summary
		// (reported P2).
		if ctx.Err() != nil && s.logger != nil {
			s.logger.Warn("mass: broadcast budget spent, remaining recipients are not attempted", "job", j.id, "channel", kind)
		}
		switch kind {
		case "aprs":
			s.massChannelAPRS(ctx, j, recipients, req.message)
		case "sms":
			s.massChannelSMS(ctx, j, recipients, req.message)
		case "email":
			s.massChannelEmail(ctx, j, recipients, req.message, req.app, s.SystemLanguage(), req.markdown())
		case "discord":
			s.massChannelDiscord(ctx, j, recipients, req.message, req.markdown())
		case "meshtastic":
			s.massChannelMeshtastic(ctx, j, recipients, req.message, req.username)
		}
		j.finishChannel(kind)
	}

	aprsN, aprsF := j.channelCounts("aprs")
	smsN, smsF := j.channelCounts("sms")
	emailN, emailF := j.channelCounts("email")
	discordN, discordF := j.channelCounts("discord")
	meshN, meshF := j.channelCounts("meshtastic")
	failures := aprsF + smsF + emailF + discordF + meshF

	unsent := j.unsentTotal()
	s.logger.Info("mass: notice broadcast", "kind", req.kind,
		"users", len(recipients), "aprs", aprsN, "sms", smsN, "email", emailN,
		"discord", discordN, "meshtastic", meshN, "failures", failures, "unsent", unsent)
	s.audit(req.username, "mass-info", fmt.Sprintf(
		"kind=%s users=%d aprs=%d sms=%d email=%d discord=%d mesh=%d failures=%d unsent=%d",
		req.kind, len(recipients), aprsN, smsN, emailN, discordN, meshN, failures, unsent))

	summary := fmt.Sprintf(i18n.T(req.lang, "mass.flash.sent"),
		len(recipients), aprsN, smsN, emailN, discordN, meshN)
	if failures > 0 {
		summary += " " + fmt.Sprintf(i18n.T(req.lang, "mass.flash.failures"), failures)
	}
	if unsent > 0 {
		// The operator must see the broadcast as INCOMPLETE, with the
		// number of messages that never went out and the reason.
		summary += " " + fmt.Sprintf(i18n.T(req.lang, "mass.flash.incomplete"), unsent)
	}
	j.finish(summary)
}

// hasAPRS reports whether a recipient has a callsign to reach.
func hasAPRS(u storage.User) bool { return len(u.APRSCallsigns) > 0 }

// hasMesh reports whether a recipient has a Meshtastic node id.
func hasMesh(u storage.User) bool { return len(u.MeshtasticIDs) > 0 }

func (s *Server) massChannelAPRS(ctx context.Context, j *massJob, recipients []storage.User, message string) {
	if s.aprs == nil {
		j.setChannelTotal("aprs", 0)
		return
	}
	j.setChannelTotal("aprs", countUsers(recipients, hasAPRS))
	for _, u := range recipients {
		if ctx.Err() != nil {
			break
		}
		if !hasAPRS(u) {
			continue
		}
		ok := true
		for _, call := range u.APRSCallsigns {
			if err := s.aprs.SendMessage(ctx, call, message); err != nil {
				ok = false
			}
		}
		j.record("aprs", ok)
	}
}

func (s *Server) massChannelSMS(ctx context.Context, j *massJob, recipients []storage.User, message string) {
	if s.gsm == nil || !s.gsm.Enabled() {
		j.setChannelTotal("sms", 0)
		return
	}
	eligible := func(u storage.User) bool { return strings.TrimSpace(u.Phone) != "" }
	j.setChannelTotal("sms", countUsers(recipients, eligible))
	for _, u := range recipients {
		if ctx.Err() != nil {
			break
		}
		if !eligible(u) {
			continue
		}
		err := s.gsm.Send(ctx, u.Phone, message)
		j.record("sms", err == nil)
	}
}

func (s *Server) massChannelMeshtastic(ctx context.Context, j *massJob, recipients []storage.User, message, sender string) {
	if s.meshtastic == nil || !s.meshtastic.Enabled() {
		j.setChannelTotal("meshtastic", 0)
		return
	}
	j.setChannelTotal("meshtastic", countUsers(recipients, hasMesh))
	for _, u := range recipients {
		if ctx.Err() != nil {
			break
		}
		if !hasMesh(u) {
			continue
		}
		ok := true
		for _, id := range u.MeshtasticIDs {
			if err := s.meshtastic.SendContactMessage(ctx, id, message, sender); err != nil {
				ok = false
			}
		}
		j.record("meshtastic", ok)
	}
}

// massMailer returns the first enabled SMTP direct mailer, if any.
func (s *Server) massMailer() (smtp.DirectMailer, bool) {
	if s.actions == nil {
		return nil, false
	}
	for _, st := range s.actions.Statuses() {
		if st.Type != smtp.Type || !st.Enabled {
			continue
		}
		p, ok := s.actions.Plugin(st.ID)
		if !ok {
			continue
		}
		if m, ok := p.(smtp.DirectMailer); ok {
			return m, true
		}
	}
	return nil, false
}

// massChannelEmail mails the notice; a markdown message is rendered into
// the HTML part instead of being shown with its markers.
func (s *Server) massChannelEmail(ctx context.Context, j *massJob, recipients []storage.User, message string, app action.AppInfo, lang string, md bool) {
	mailer, ok := s.massMailer()
	if !ok {
		j.setChannelTotal("email", 0)
		return
	}
	subject := i18n.T(lang, "mass.mail_subject")
	if h := strings.TrimSpace(app.Header1); h != "" {
		subject = "[" + h + "] " + subject
	}
	eligible := func(u storage.User) bool { return strings.TrimSpace(u.Email) != "" }
	j.setChannelTotal("email", countUsers(recipients, eligible))
	for _, u := range recipients {
		if ctx.Err() != nil {
			break
		}
		if !eligible(u) {
			continue
		}
		var err error
		if md {
			err = mailer.SendNoticeMarkdown(ctx, []string{u.Email}, subject, message, app, lang)
		} else {
			err = mailer.SendNotice(ctx, []string{u.Email}, subject, message, app, lang)
		}
		j.record("email", err == nil)
	}
}

// discordDMAvailable reports whether any enabled Discord instance can send
// a direct message (a bot token is configured).
func (s *Server) discordDMAvailable() bool {
	_, ok := s.massDiscordDM()
	return ok
}

// massDiscordDM returns the first enabled Discord instance that can send
// direct messages (i.e. one configured with a bot token).
func (s *Server) massDiscordDM() (discord.DirectMessenger, bool) {
	if s.actions == nil {
		return nil, false
	}
	for _, st := range s.actions.Statuses() {
		if st.Type != discord.Type || !st.Enabled {
			continue
		}
		p, ok := s.actions.Plugin(st.ID)
		if !ok {
			continue
		}
		if dm, ok := discord.DirectMessengerOf(p); ok {
			return dm, true
		}
	}
	return nil, false
}

// massChannelDiscord delivers the notice as a DIRECT MESSAGE to each
// recipient's own Discord id (the profile's Discord field): a mass notice
// is personal, and a channel-wide post would notify everyone who happens
// to be in the channel — including people who never asked for anything.
// The id must be a numeric user id; profile values that are usernames or
// handles fail for that recipient, which the panel shows as a failure. A
// markdown message is rewritten into what Discord renders (headings have
// no syntax there, so they become bold lines).
func (s *Server) massChannelDiscord(ctx context.Context, j *massJob, recipients []storage.User, message string, md bool) {
	dm, ok := s.massDiscordDM()
	if !ok {
		// No bot token anywhere: Discord cannot be delivered to
		// individual users, and the mass panel never posts on the
		// channel instead.
		j.setChannelTotal("discord", 0)
		if s.logger != nil {
			s.logger.Warn("mass: discord skipped (no bot token configured for direct messages)")
		}
		return
	}
	if md {
		message = markdown.Discord(message)
	}
	eligible := func(u storage.User) bool { return strings.TrimSpace(u.Discord) != "" }
	j.setChannelTotal("discord", countUsers(recipients, eligible))
	for _, u := range recipients {
		if ctx.Err() != nil {
			break
		}
		if !eligible(u) {
			continue
		}
		err := dm.SendDM(ctx, u.Discord, message)
		if err != nil && s.logger != nil {
			s.logger.Warn("mass: discord direct message failed", "user", u.Username, "error", err)
		}
		j.record("discord", err == nil)
	}
}

// countUsers returns how many recipients satisfy pred.
func countUsers(recipients []storage.User, pred func(storage.User) bool) int {
	n := 0
	for _, u := range recipients {
		if pred(u) {
			n++
		}
	}
	return n
}

// handleMassProgress renders the live progress fragment for one job (the
// page polls it while the broadcast runs).
func (s *Server) handleMassProgress(w http.ResponseWriter, r *http.Request) {
	job := s.massJobs.get(strings.TrimSpace(r.URL.Query().Get("job")))
	if job == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.render(w, "mass_progress", job.progressView(s.langFor(r)))
}
