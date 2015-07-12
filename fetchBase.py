
import database as db
import webFunctions
import logging
import traceback
import sqlalchemy.exc
import runstate
import urllib.error
import urllib.parse
import re
import parsedatetime
import hashlib
import os
import settings
import os.path
import time
import datetime

import abc

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
		self.wg = webFunctions.WebGetRobust()

		# db.session = db.Session()

	def get_job(self):
		while 1:
			try:
				job = db.session.query(db.Releases)               \
					.filter(db.Releases.dlstate == 0)             \
					.filter(db.Releases.source == self.pluginkey) \
					.order_by(db.Releases.postid)                 \
					.limit(1)                                     \
					.one()
				if job == None:
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
			self.log.warn("Cache directory for book items did not exist. Creating")
			self.log.warn("Directory at path '%s'", settings.storeDir)
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





def run(indice):
	print("Runner {}!".format(indice))
	fetcher = DanbooruFetcher()
	remainingTasks = True

	try:
		while remainingTasks and runstate.run:
			remainingTasks = fetcher.retreiveItem()
	except KeyboardInterrupt:
		return
	except:
		print("Unhandled exception!")
		traceback.print_exc()
		raise
