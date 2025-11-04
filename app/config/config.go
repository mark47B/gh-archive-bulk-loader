package config

type Config struct {
	MongoURI  string
	DBName    string
	CollName  string
	RedisAddr string
	RedisDB   int
}

type LoaderConfig struct {
	ProducerWorkers int
	ConsumerWorkers int
	BatchSize       int
}

type ReplicaLeaderConfig struct {
	LeaderKey   string
	ReplicaID   string
	LeaderLease int
}
