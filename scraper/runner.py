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



PLUGIN_CLASSES = [

	# Ok:
	scraper.modules.e621Scrape.E621Fetcher,
	scraper.modules.KonaChanFetch.KonaChanFetcher,
	scraper.modules.r34xxxScrape.R34xxxFetcher,
	scraper.modules.danbooruFetch.DanbooruFetcher,
	scraper.modules.tbibFetch.TbibFetcher,
	scraper.modules.xbooruFetch.XBooruFetcher,

	# Fucked:
	# scraper.modules.gelbooruFetch.GelbooruFetcher,
]

class RunEngine(object):
	def __init__(self):
		self.log = logging.getLogger("Main.Runner")

	def run(self):
		self.log.info("Inserting start URLs")


		self.log.info("Creating run contexts")


		# for plugin in PLUGIN_CLASSES:
		# 	plugin.run_scraper()

		threads = []
		try:
			for plugin in PLUGIN_CLASSES:
				th = threading.Thread(target=plugin.run_scraper, name=plugin.loggerpath)
				threads.append(th)


			for thread in threads:
				thread.start()

			self.log.info("Waiting for workers to complete.")
			for thread in threads:
				thread.join()
		except KeyboardInterrupt:
			self.log.info("Waiting for executor.")
			scraper.runstate.run = False
			for thread in threads:
				thread.join()

def go():
	instance = RunEngine()
	instance.run()

