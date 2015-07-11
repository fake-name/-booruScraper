
import database as db
import logSetup
logSetup.initLogging()

import fetcher
import runstate
import concurrent.futures

THREADS = 10

def insertStartingPoints():

	tmp = db.session.query(db.Releases) \
		.filter(db.Releases.postid == 1)  \
		.count()
	if not tmp:
		for x in range(2070000):
			new = db.Releases(dlstate=0, postid=x)
			db.session.add(new)
			if x % 10000 == 0:
				print("Loop ", x, "flushing...")
				db.session.flush()
				print("Flushed.")
	db.session.commit()


def resetDlstate():
	tmp = db.session.query(db.Releases) \
	.filter(db.Releases.dlstate == 1)   \
	.update({db.Releases.dlstate : 0})
	db.session.commit()


def go():
	insertStartingPoints()
	resetDlstate()

	executor = concurrent.futures.ThreadPoolExecutor(max_workers=THREADS)
	try:
		# for x in range(THREADS):
		for x in range(1):
			executor.submit(fetcher.run, x)
		executor.shutdown()
	except KeyboardInterrupt:
		print("Waiting for executor.")
		runstate.run = False
		executor.shutdown()




if __name__ == '__main__':
	go()