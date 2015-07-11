
import database as db
import webFunctions
import logging
import traceback
import sqlalchemy.exc
import runstate

class DanbooruFetcher(object):
	def __init__(self):
		self.log = logging.getLogger("Main.Danbooru")
		self.wg = webFunctions.WebGetRobust()

		self.session = db.Session()

	def get_job(self):
		while 1:
			try:
				job = self.session.query(db.Releases)   \
					.filter(db.Releases.dlstate == 0) \
					.order_by(db.Releases.postid)     \
					.limit(1)                         \
					.one()
				if job == None:
					return None
				job.dlstate = 1
				self.session.commit()
				return job
			except sqlalchemy.exc.DatabaseError:
				self.log.warning("Error when getting job. Probably a concurrency issue.")
				self.log.warning("Trying again.")
				for line in traceback.format_exc().split("\n"):
					self.log.warning(line)
				self.session.rollback()

	def retreiveItem(self):
		job = self.get_job()
		if not job:
			return False


		return True





def run(indice):
	print("Runner {}!".format(indice))
	fetcher = DanbooruFetcher()
	job = True

	try:
		while job and runstate.run:
			job = fetcher.get_job()
			print("Have job: ", job, job.postid)
	except KeyboardInterrupt:
		return
