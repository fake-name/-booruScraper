
import sys
import database as db
import logSetup
logSetup.initLogging()

import danbooruFetch
import gelbooruFetch
import runstate
import concurrent.futures

THREADS = 1
THREADS = 30

def insertDanbooruStartingPoints():

	tmp = db.session.query(db.Releases)           \
		.filter(db.Releases.postid == 1)          \
		.filter(db.Releases.source == 'Danbooru') \
		.count()
	if not tmp:
		for x in range(2070000):
			new = db.Releases(dlstate=0, postid=x, source='Danbooru')
			db.session.add(new)
			if x % 10000 == 0:
				print("Loop ", x, "flushing...")
				db.session.flush()
				print("Flushed.")
	db.session.commit()

def insertGelbooruStartingPoints():

	tmp = db.session.query(db.Releases)           \
		.filter(db.Releases.postid == 1)          \
		.filter(db.Releases.source == 'Gelbooru') \
		.count()
	if not tmp:

		print("Building insert data structure")
		dat = [{"dlstate" : 0, "postid" : x, "source" : 'Gelbooru'} for x in range(2900000)]
		print("Building insert query")
		q = db.Releases.__table__.insert().values(dat)
		print("Built. Doing insert.")
		db.engine.execute(q)
		print("Done.")
		# for x in :

			# new = db.Releases(dlstate=0, postid=x, source='Gelbooru')
			# # db.session.add(new)
			# if x % 100000 == 0:
			# 	print("Loop ", x, "flushing...")
			# 	db.session.flush()
			# 	print("Flushed.")
	db.session.commit()


def resetDlstate():
	tmp = db.session.query(db.Releases) \
	.filter(db.Releases.dlstate == 1)   \
	.update({db.Releases.dlstate : 0})
	db.session.commit()


def go():
	insertDanbooruStartingPoints()
	insertGelbooruStartingPoints()
	resetDlstate()




	executor = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)
	try:
		# for x in range(2):
		for x in range(THREADS//2):
			executor.submit(danbooruFetch.run, x)
		for x in range(THREADS//2):
			executor.submit(gelbooruFetch.run, x)
		executor.shutdown()
	except KeyboardInterrupt:
		print("Waiting for executor.")
		runstate.run = False
		executor.shutdown()




if __name__ == '__main__':
	go()