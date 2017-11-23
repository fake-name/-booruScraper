import logging


import threading

import scraper.database as db
import scraper.runstate

import scraper.modules.danbooruFetch
import scraper.modules.gelbooruFetch
import scraper.modules.r34xxxScrape
import scraper.modules.KonaChanFetch
import scraper.modules.e621Scrape
import scraper.modules.tbibFetch
import scraper.modules.xbooruFetch


# THREADS = 6
THREADS = 15


PLUGIN_CLASSES = [
	scraper.modules.danbooruFetch.DanbooruFetcher,
	# scraper.modules.gelbooruFetch.GelbooruFetcher,
	scraper.modules.r34xxxScrape.R34xxxFetcher,
	scraper.modules.KonaChanFetch.KonaChanFetcher,
	scraper.modules.e621Scrape.E621Fetcher,
	scraper.modules.tbibFetch.TbibFetcher,
	scraper.modules.xbooruFetch.XBooruFetcher,
]

class RunEngine(object):
	def __init__(self, worker_count):
		self.log = logging.getLogger("Main.Runner")
		self.workers = worker_count



	def run(self):
		self.log.info("Inserting start URLs")


		self.log.info("Creating run contexts")


		for plugin in PLUGIN_CLASSES:
			plugin.run_scraper()

		# threads = []
		# try:
		# 	for plugin in PLUGIN_CLASSES:
		# 		th = threading.Thread(target=plugin.run_scraper, name=plugin.loggerpath)
		# 		threads.append(th)


		# 	for thread in threads:
		# 		thread.start()

		# 	self.log.info("Waiting for workers to complete.")
		# 	for thread in threads:
		# 		thread.join()
		# except KeyboardInterrupt:
		# 	self.log.info("Waiting for executor.")
		# 	scraper.runstate.run = False
		# 	for thread in threads:
		# 		thread.join()

def go():
	instance = RunEngine(THREADS)
	instance.run()

