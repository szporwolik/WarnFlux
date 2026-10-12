package smtp_test
import("context";"fmt";"net";"testing";"time";"github.com/szporwolik/WarnFlux/internal/actions/smtp";"github.com/szporwolik/WarnFlux/internal/action")
func TestReview7NoticeIgnoresRateLimit(t *testing.T){
 srv:=newSMTPServer(t,nil)
 host,port,_:=net.SplitHostPort(srv.addr())
 p,err:=smtp.New(cfgNode(t,fmt.Sprintf("host: %s\nport: %s\nfrom: from@example.net\nto: [a@example.net]\nstarttls: false\nrate_limit_per_minute: 60\n",host,port)))
 if err!=nil{t.Fatal(err)}
 ctx,cancel:=context.WithTimeout(context.Background(),300*time.Millisecond);defer cancel()
 mailer:=p.(smtp.DirectMailer)
 for _,to:=range []string{"a@example.net","b@example.net"}{
  if err:=mailer.SendNotice(ctx,[]string{to},"notice","body",action.AppInfo{},"en");err!=nil{return}
 }
 sent:=srv.snapshot()
 if len(sent)!=2{t.Fatalf("setup: %d deliveries",len(sent))}
 if delta:=sent[1].at.Sub(sent[0].at);delta<time.Second{t.Fatalf("60/minute limit bypassed: two notices %s apart",delta)}
}
