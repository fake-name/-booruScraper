import logging


import concurrent.futures
from sqlalchemy.dialects.postgresql import insert

import scraper.database as db
import scraper.runstate

import scraper.modules.danbooruFetch
import scraper.modules.gelbooruFetch
import scraper.modules.r34xxxScrape
import scraper.modules.KonaChanFetch
import scraper.modules.e621Scrape


# THREADS = 6
THREADS = 15

UPSERT_STEP = 10000

PLUGIN_CLASSES = [
	scraper.modules.danbooruFetch.DanbooruFetcher,
	scraper.modules.gelbooruFetch.GelbooruFetcher,
	scraper.modules.r34xxxScrape.R34xxxFetcher,
	scraper.modules.KonaChanFetch.KonaChanFetcher,
	scraper.modules.e621Scrape.E621Fetcher,
]

class RunEngine(object):
	def __init__(self, worker_count):
		self.log = logging.getLogger("Main.Runner")
		self.workers = worker_count


	def resetDlstate(self):
		tmp = db.session.query(db.Releases)     \
			.filter(db.Releases.dlstate == 1)   \
			.update({db.Releases.dlstate : 0})
		db.session.commit()


	def do_upsert(self, target, maxitems):
		for x in range(maxitems, 0, UPSERT_STEP * -1):

			self.log.info("[%s] - Building insert data structure %s -> %s", target, x, x+UPSERT_STEP)
			dat = [{"dlstate" : 0, "postid" : x, "source" : target} for x in range(x, x+UPSERT_STEP)]
			self.log.info("[%s] - Building insert query", target)
			q = insert(db.Releases).values(dat)
			q = q.on_conflict_do_nothing()
			self.log.info("[%s] - Built. Doing insert.", target)
			ret = db.session.execute(q)

			changes = ret.rowcount
			self.log.info("[%s] - Changed rows: %s", target, changes)
			db.session.commit()

			if not changes:
				break
		self.log.info("[%s] - Done.", target)


	def run(self):
		self.log.info("Inserting start URLs")


		self.do_upsert("Danbooru", 2750000)
		self.do_upsert('Gelbooru', 3650000)
		self.do_upsert('Rule34.xxx', 2300000)
		self.do_upsert('e621', 1200000)
		self.do_upsert('KonaChan', 245000)

		self.log.info("Resetting DL states.")
		# resetDlstate()

		self.log.info("Creating run contexts")
		executor = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)


		try:
			for plugin in PLUGIN_CLASSES:
				for x in range(50):
					executor.submit(plugin.run_scraper, x)


			self.log.info("Waiting for workers to complete.")
			executor.shutdown()
		except KeyboardInterrupt:
			self.log.info("Waiting for executor.")
			scraper.runstate.run = False
			executor.shutdown()

def go():
	instance = RunEngine(THREADS)
	instance.run()

