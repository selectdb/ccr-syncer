package version

// Git SHA Value will be set during build
var GitTagSha = "Git tag sha: Not provided, use Makefile to build"

func GetVersion() string {
	return GitTagSha
}
