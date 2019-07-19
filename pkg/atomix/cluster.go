package atomix

// Cluster describes the structure of the Atomix cluster
type Cluster struct {
	MemberID string
	Members  map[string]Member
}

// Member describes a single member of the Atomix cluster
type Member struct {
	ID   string
	Host string
	Port int
}
