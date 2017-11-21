
import sys
import database as db

from sqlalchemy.dialects.postgresql import insert

import logSetup
logSetup.initLogging()

import danbooruFetch
import gelbooruFetch
import r34xxxScrape
import KonaChanFetch
import e621Scrape
import runstate
import concurrent.futures

# THREADS = 6
THREADS = 15

UPSERT_STEP = 10000

def do_upsert(target, maxitems):
	for x in range(maxitems, 0, UPSERT_STEP * -1):

		print("[%s] - Building insert data structure %s -> %s" % (target, x, x+UPSERT_STEP))
		dat = [{"dlstate" : 0, "postid" : x, "source" : target} for x in range(x, x+UPSERT_STEP)]
		print("[%s] - Building insert query" % target)
		q = insert(db.Releases).values(dat)
		q = q.on_conflict_do_nothing()
		print("[%s] - Built. Doing insert." % target)
		ret = db.session.execute(q)

		changes = ret.rowcount
		print("[%s] - Changed rows: %s" % (target, changes))
		db.session.commit()

		if not changes:
			break
	print("[%s] - Done." % target)


def resetDlstate():
	tmp = db.session.query(db.Releases) \
	.filter(db.Releases.dlstate == 1)   \
	.update({db.Releases.dlstate : 0})
	db.session.commit()


def go():
	print("Inserting start URLs")

	do_upsert("Danbooru", 2750000)
	do_upsert('Gelbooru', 3650000)
	do_upsert('Rule34.xxx', 2300000)
	do_upsert('e621', 1200000)
	do_upsert('KonaChan', 245000)

	print("Resetting DL states.")
	# resetDlstate()

	print("Creating run contexts")
	executor = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)

	plugins = [r34xxxScrape, danbooruFetch, gelbooruFetch, e621Scrape, KonaChanFetch]

	try:
		for plugin in plugins:
			for x in range(50):
				executor.submit(plugin.run, x)


		print("Waiting for workers to complete.")
		executor.shutdown()
	except KeyboardInterrupt:
		print("Waiting for executor.")
		runstate.run = False
		executor.shutdown()




if __name__ == '__main__':
	go()