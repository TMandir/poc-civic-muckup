package database

// defaultDatabasePath keeps persistent application data outside source packages.
const defaultDatabasePath = "./data/leveldb"

// OpenDefault opens or creates the application's LevelDB database in the default data directory.
func OpenDefault() (*Database, error) {
	return Open(defaultDatabasePath)
}
