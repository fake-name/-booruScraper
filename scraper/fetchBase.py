

import logging
import traceback
import os
import os.path
import abc
import hashlib


import sqlalchemy.exc

import settings
import util.WebRequest
import scraper.runstate
import scraper.database as db

class AbstractFetcher(object, metaclass=abc.ABCMeta):

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


		# db.session = db.Session()

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


	def saveFile(self, row, filename, fileCont):
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



	def saveFile(self, filename, fileCont):
		if not os.path.exists(settings.storeDir):
			self.log.warn("Cache directory for book items did not exist. Creating")
			self.log.warn("Directory at path '%s'", settings.storeDir)
			os.makedirs(settings.storeDir)


		fHash, ext = os.path.splitext(filename)

		ext   = ext.lower()
		fHash = fHash.upper()

		# use the first 3 chars of the hash for the folder name.
		# Since it's hex-encoded, that gives us a max of 2^12 bits of
		# directories, or 4096 dirs.
		dirName = fHash[:3]

		dirPath = os.path.join(settings.storeDir, dirName)
		if not os.path.exists(dirPath):
			os.makedirs(dirPath)

		ext = os.path.splitext(filename)[-1]

		ext   = ext.lower()
		fHash = fHash.upper()

		# The "." is part of the ext.
		filename = '{filename}{ext}'.format(filename=fHash, ext=ext)

		fqpath = os.path.join(dirPath, filename)
		fqpath = os.path.abspath(fqpath)
		if not fqpath.startswith(settings.storeDir):
			raise ValueError("Generating the file path to save a cover produced a path that did not include the storage directory?")

		locpath = fqpath[len(settings.storeDir):]

		with open(fqpath, "wb") as fp:
			fp.write(fileCont)

		return locpath




	@classmethod
	def run_scraper(cls, indice):
		print("Runner {}!".format(indice))
		fetcher = cls()
		remainingTasks = True

		try:
			while remainingTasks and scraper.runstate.run:
				remainingTasks = fetcher.retreiveItem()
		except KeyboardInterrupt:
			return
		except:
			print("Unhandled exception!")
			traceback.print_exc()
			raise
