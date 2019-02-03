

import logging
import traceback
import os
import os.path
import abc
import hashlib
import time
import random
import concurrent.futures

import tqdm
import sqlalchemy.exc
from sqlalchemy import desc
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

import settings
import WebRequest
import scraper.runstate
import scraper.database as db

class AbstractFetcher(object, metaclass=abc.ABCMeta):

	worker_threads = 16

	@abc.abstractmethod
	def get_content_count_max(self, job):
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
		self.wg = WebRequest.WebGetRobust(logPath=self.loggerpath+".Web")

		self.jobs_queued = []


	def get_job(self):

		session = db.session()
		print("Cursor: ", session)
		while 1:
			self.log.info("Getting job")
			try:
				if not self.jobs_queued:
					raw_query = '''
							UPDATE
							    db_releases
							SET
							    state = 'fetching'
							WHERE
							    db_releases.id in (
							        SELECT
							            db_releases.id
							        FROM
							            db_releases
							        WHERE
							            db_releases.state = 'new'
							        AND
							           source = :source
							        ORDER BY
							            db_releases.postid ASC
							        LIMIT 500
							    )
							AND
							    db_releases.state = 'new'
							RETURNING
							    db_releases.id;
						'''

					rids = session.execute(text(raw_query), {'source' : self.pluginkey})
					ridl = list(rids)
					self.jobs_queued = [tmp[0] for tmp in ridl]

				assert self.jobs_queued

				rid = self.jobs_queued.pop()


				job = db.session.query(db.Releases)               \
					.filter(db.Releases.id == rid)

				job = job.scalar()
				if job is None:
					return None
				job.state = 'fetching'
				db.session.commit()
				return job
			except sqlalchemy.exc.OperationalError as e:
				db.session.rollback()
			except sqlalchemy.exc.DatabaseError as e:
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
		try:
			while self.retreiveItem():
				pass
		except Exception as e:
			self.log.error("Worker thread had exception")
			for line in traceback.format_exc().split("\n"):
				self.log.error(line)


	def resetDlstate(self):

		sess = db.session()
		sess.query(db.Releases)                                                           \
			.filter(db.Releases.state == 'fetching' or db.Releases.state == 'processing') \
			.filter(db.Releases.source == self.pluginkey)                                 \
			.update({db.Releases.state : 'new'})

		sess.commit()


	def do_upsert(self):
		UPSERT_STEP = 1000
		sess = db.session()
		total_changes = 0

		item_count = self.get_content_count_max()
		self.log.info("Max source items = %s", item_count)
		pbar = tqdm.tqdm(range(item_count + UPSERT_STEP, -1, UPSERT_STEP * -1))
		for x in pbar:

			# self.log.info("[%s] - Building insert data structure %s -> %s", self.pluginkey, x, x+UPSERT_STEP)
			dat = [{"state" : 'new', "postid" : x, "source" : self.pluginkey} for x in range(x, x+UPSERT_STEP)]
			# self.log.info("[%s] - Building insert query", self.pluginkey)
			q = insert(db.Releases).values(dat)
			q = q.on_conflict_do_nothing()
			# self.log.info("[%s] - Built. Doing insert.", self.pluginkey)
			ret = sess.execute(q)

			changes = ret.rowcount
			total_changes += changes
			# if changes != UPSERT_STEP:
			# 	self.log.info("[%s] - Changed rows: %s", self.pluginkey, changes)
			if changes:
				sess.commit()
			pbar.set_description("Changes: %s (%s)" % (changes, total_changes))
			if not changes:
				break
		self.log.info("[%s] - Done.", self.pluginkey)


	def __go(self):
		self.resetDlstate()

		executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_threads)
		try:
			self.log.info("Staggered-Launching %s threads", self.worker_threads)
			for _ in range(self.worker_threads):
				executor.submit(self.run_worker)
				time.sleep(random.uniform(0.2, 4.0))

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
