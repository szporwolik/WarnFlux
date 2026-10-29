package storage

// DigestCounters is the traffic of one reporting window, read by the daily
// admin status mail. The zero value is valid and means "nothing happened".
type DigestCounters struct {
	APRSReceived int
	APRSSent     int
	SMSReceived  int
	SMSSent      int
	MeshReceived int
	MeshSent     int
	// HazardsNew counts hazards first seen in the window, HazardsActive the
	// ones still active now, HazardsClosed the ones cancelled or expired.
	HazardsNew    int
	HazardsActive int
	HazardsClosed int
	// Deliveries counts the notifications dispatched in the window and how
	// many of them failed.
	Deliveries       int
	DeliveriesFailed int
}
