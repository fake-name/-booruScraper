import logging

import threading
import multiprocessing
import concurrent.futures

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
	scraper.modules.danbooruFetch.DanbooruFetcher,
	scraper.modules.KonaChanFetch.KonaChanFetcher,
	scraper.modules.r34xxxScrape.R34xxxFetcher,
	scraper.modules.tbibFetch.TbibFetcher,
	scraper.modules.xbooruFetch.XBooruFetcher,
	scraper.modules.gelbooruFetch.GelbooruFetcher,

]

class RunEngine(object):
	def __init__(self):
		self.log = logging.getLogger("Main.Runner")

	def run(self):
		self.log.info("Inserting start URLs")

		for plugin in PLUGIN_CLASSES:
			instance = plugin()
			instance.do_upsert()

		self.log.info("Creating run contexts")



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
			scraper.runstate.run = False
			self.log.info("Waiting for threads to join.")
			for thread in threads:
				thread.join()


	def run_sequential(self):
		self.log.info("Inserting start URLs")

		for plugin in PLUGIN_CLASSES:
			instance = plugin()
			instance.do_upsert()


		self.log.info("Doing sequential execution")


		try:
			for plugin in PLUGIN_CLASSES:
				plugin.run_scraper()

		except KeyboardInterrupt:
			scraper.runstate.run = False
			self.log.info("Waiting for threads to join.")

	def run_shared_pool(self):
		self.log.info("Inserting start URLs")

		for plugin in PLUGIN_CLASSES:
			instance = plugin()
			instance.do_upsert()


		self.log.info("Doing sequential execution")

		worker_threads = db.DB_CONNECTION_POOL_SIZE * 2

		executor = concurrent.futures.ThreadPoolExecutor(max_workers=db.DB_CONNECTION_POOL_SIZE)
		try:
			self.log.info("Staggered-Launching %s threads", worker_threads)

			for plugin in PLUGIN_CLASSES:
				for _ in range(db.DB_CONNECTION_POOL_SIZE // 2):
					executor.submit(plugin.run_single_thread)

		except KeyboardInterrupt:
			scraper.runstate.run = False
			self.log.info("Waiting for threads to join.")

def go():
	instance = RunEngine()
	# instance.run()
	# instance.run_sequential()
	instance.run_shared_pool()

