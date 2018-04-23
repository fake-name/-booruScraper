
import logging
import settings
import tqdm
import os.path

from sqlalchemy.sql.expression import func

import scraper.database as db

class TestEngine():
	def __init__(self):
		self.log = logging.getLogger("Main.Runner")
	def check_dir(self):
		self.log.info("Checking dir")
		dir_ok1 = os.path.exists(settings.storeDir)
		dir_ok2 = os.path.isdir(settings.storeDir)
		if not dir_ok1:
			self.log.error("Download dir (%s) doesn't exist!", settings.storeDir)
		elif not dir_ok2:
			self.log.error("Download dir (%s) isn't a folder!", settings.storeDir)
		else:
			self.log.info("Download dir appears to be OK")

		dirc = os.listdir(settings.storeDir)
		if not all([len(tmp) == 3 for tmp in dirc]):
			self.log.error("There appears to be a non-hash-derived filename in the download store dir!")
		else:
			self.log.info("Download store dir looks like it's contents are valid!")


	def check_db(self):
		session = db.session()

		self.log.info("Loading rows from DB")
		test_items = db.session.query(db.Files.filepath)  \
			.order_by(func.random())                      \
			.limit(10000)                                 \
			.all()

		self.log.info("Selected %s rows.", len(test_items))

		had_bad = False
		for filepath,  in tqdm.tqdm(test_items):
			fqp = os.path.join(settings.storeDir, filepath)
			if not os.path.exists(fqp):
				self.log.error("File that should exists is missing (%s, %s, %s)!", filepath, fqp, os.path.exists(fqp))
				had_bad = True

		if had_bad:
			self.log.error("Had a missing file!")
		else:
			self.log.info("All checked files passed validation!")



	def run(self):
		self.check_dir()
		self.check_db()


def go():
	instance = TestEngine()
	instance.run()

