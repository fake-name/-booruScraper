

import logging
import traceback
import os
import os.path
import abc
import hashlib
import concurrent.futures

import sqlalchemy.exc
from sqlalchemy.dialects.postgresql import insert

import settings
import util.WebRequest
import scraper.runstate
import scraper.database as db

class AbstractFetcher(object, metaclass=abc.ABCMeta):

	worker_threads = 6

	@abc.abstractproperty
	def content_count_max(self):
		pass

	@abc.abstractproperty
	def loggerpath(self):
		pass

	@abc.abstractproperty
	def pluginkey(self):
		pass

	@abc.abstractmethod
	def processJob(self, job):
		pass


	def __init__(self):
		self.log = logging.getLogger(self.loggerpath)
		self.wg = util.WebRequest.WebGetRobust(logPath=self.loggerpath+".Web")



	def get_job(self):
		while 1:
			try:
				job = db.session.query(db.Releases)               \
					.filter(db.Releases.source == self.pluginkey) \
					.filter(db.Releases.dlstate == 0)             \
					.order_by(db.Releases.postid)                 \
					.limit(1)

				job = job.scalar()
				if job is None:
					return None
				job.dlstate = 1
				db.session.commit()
				return job
			except sqlalchemy.exc.DatabaseError:
				self.log.warning("Error when getting job. Probably a concurrency issue.")
				self.log.warning("Trying again.")
				for line in traceback.format_exc().split("\n"):
					self.log.warning(line)
				db.session.rollback()


	def saveFileRow(self, row, filename, fileCont):
		if not os.path.exists(settings.storeDir):
			self.log.warning("Cache directory for book items did not exist. Creating")
			self.log.warning("Directory at path '%s'", settings.storeDir)
			os.makedirs(settings.storeDir)



		ext = os.path.splitext(filename)[-1]
		fhash = hashlib.md5(fileCont).hexdigest()

		ext   = ext.lower()
		fhash = fhash.upper()

		# use the first 3 chars of the hash for the folder name.
		# Since it's hex-encoded, that gives us a max of 2^12 bits of
		# directories, or 4096 dirs.
		dirName = fhash[:3]

		dirPath = os.path.join(settings.storeDir, dirName)
		if not os.path.exists(dirPath):
			os.makedirs(dirPath)


		# The "." is part of the ext.
		filename = '{filename}{ext}'.format(filename=fhash, ext=ext)

		fqpath = os.path.join(dirPath, filename)
		fqpath = os.path.abspath(fqpath)
		if not fqpath.startswith(settings.storeDir):
			raise ValueError("Generating the file path to save a cover produced a path that did not include the storage directory?")

		locpath = fqpath[len(settings.storeDir):]

		row.file.append((locpath, fhash))


		with open(fqpath, "wb") as fp:
			fp.write(fileCont)

		return locpath


	def retreiveItem(self):
		job = self.get_job()
		if not job:
			return False

		self.processJob(job)
		return True



	def run_worker(self):
		pass

	def resetDlstate(self):

		sess = db.session()
		tmp = sess.query(db.Releases)                     \
			.filter(db.Releases.dlstate == 1)             \
			.filter(db.Releases.source == self.pluginkey) \
			.update({db.Releases.dlstate : 0})            \

		sess.commit()


	def do_upsert(self):
		UPSERT_STEP = 10000
		sess = db.session()

		for x in range(self.content_count_max, 0, UPSERT_STEP * -1):

			self.log.info("[%s] - Building insert data structure %s -> %s", self.pluginkey, x, x+UPSERT_STEP)
			dat = [{"dlstate" : 0, "postid" : x, "source" : self.pluginkey} for x in range(x, x+UPSERT_STEP)]
			self.log.info("[%s] - Building insert query", self.pluginkey)
			q = insert(db.Releases).values(dat)
			q = q.on_conflict_do_nothing()
			self.log.info("[%s] - Built. Doing insert.", self.pluginkey)
			ret = sess.execute(q)

			changes = ret.rowcount
			self.log.info("[%s] - Changed rows: %s", self.pluginkey, changes)
			sess.commit()

			if not changes:
				break
		self.log.info("[%s] - Done.", self.pluginkey)


	def __go(self):
		self.resetDlstate()
		self.do_upsert()

		executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_threads)
		try:
			self.log.info("Launching %s threads", self.worker_threads)
			for _ in range(self.worker_threads):
				executor.submit(self.run_worker)

			self.log.info("%s threads launched, blocking while workers finish", self.worker_threads)
			executor.shutdown()
			self.log.info("Workers have finished")
		except KeyboardInterrupt:
			self.log.info("Waiting for executor.")
			scraper.runstate.run = False
			executor.shutdown()


	@classmethod
	def run_scraper(cls):
		print("Runner {}!".format(cls.pluginkey))
		fetcher = cls()
		fetcher.__go()
