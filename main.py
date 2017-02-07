
import sys
import database as db

from sqlalchemy.dialects.postgresql import insert

import logSetup
logSetup.initLogging()

import danbooruFetch
import gelbooruFetch
import r34xxxScrape
import runstate
import concurrent.futures

# THREADS = 6
THREADS = 30

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

	do_upsert("Danbooru", 2700000)
	do_upsert('Gelbooru', 3600000)
	do_upsert('Rule34.xxx', 2300000)

	print("Resetting DL states.")
	resetDlstate()

	print("Creating run contexts")
	executor = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)
	try:
		# for x in range(2):
		# executor.submit(danbooruFetch.run, 0)
		# executor.submit(gelbooruFetch.run, 0)
		for x in range(THREADS//3):
			executor.submit(r34xxxScrape.run, x)
		for x in range(THREADS//2):
			executor.submit(danbooruFetch.run, x)
		for x in range(THREADS//2):
			executor.submit(gelbooruFetch.run, x)

		print("Waiting for workers to complete.")
		executor.shutdown()
	except KeyboardInterrupt:
		print("Waiting for executor.")
		runstate.run = False
		executor.shutdown()




if __name__ == '__main__':
	go()