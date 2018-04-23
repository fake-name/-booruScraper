
import sys
import util.logSetup
import scraper.runner
import scraper.conf_validate


if __name__ == '__main__':
	util.logSetup.initLogging()

	if "probe" in sys.argv:
		scraper.conf_validate.go()
	else:
		scraper.runner.go()
