
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
import os
import settings
import os.path
import time
import datetime

class GelbooruFetcher(object):
	def __init__(self):
		self.log = logging.getLogger("Main.Gelbooru")
		self.wg = webFunctions.WebGetRobust(logPath="Main.Gelbooru.Web")

		# db.session = db.Session()

	def get_job(self):
		while 1:
			try:
				job = db.session.query(db.Releases)   \
					.filter(db.Releases.dlstate == 0) \
					.order_by(db.Releases.postid)     \
					.limit(1)                         \
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

	def extractTags(self, job, tagsection):

		taglis       = tagsection.find_all('li', class_='tag-type-general')
		characterlis = tagsection.find_all('li', class_='tag-type-character')
		artistlis    = tagsection.find_all('li', class_='tag-type-artist')


		tags = []
		for tagli in taglis:
			tag = tagli.find_all('a')[1].get_text()
			tags.append(tag)

		artists = []
		for artistli in artistlis:
			artist = artistli.find_all('a')[1].get_text()
			artists.append(artist)

		characters = []
		for characterli in characterlis:
			character = characterli.find_all('a')[1].get_text()
			characters.append(character)


		for tag in tags:
			if tag not in job.tags:
				job.tags.append(tag)
		for artist in artists:
			if artist not in job.artist:
				job.artist.append(artist)
		for character in characters:
			if character not in job.character:
				job.character.append(character)

	def getxy(self, instr):
		found = re.search(r"(\d+)x(\d+)", instr)
		x, y = found.groups()
		return x, y

	def extractInfo(self, job, infosection):

		for li in infosection.find_all("li"):
			rawt = li.get_text().strip()
			if not rawt:
				continue
			if not ":" in rawt:
				print("rawt: '{}'".format(rawt))

			name, val = rawt.split(":", 1)

			name = name.strip()
			val = val.strip()

			if name == 'Rating':
				job.rating = val
			elif name == 'Favorites':
				job.favorites = val
			elif name == 'Score':
				job.score = val.split()[0]
			elif name == 'Posted':
				cal = parsedatetime.Calendar()
				itemdate =      val.split("at")[0]
				itemdate = itemdate.split("by")[0]
				print("itemdate", itemdate)
				tstruct, pstat = cal.parse(itemdate)
				print("Ret: ", pstat, tstruct)
				assert pstat == 1 or pstat == 2 or pstat == 3
				job.posted = datetime.datetime.fromtimestamp(time.mktime(tstruct))
			elif name == 'Size':
				job.imgx, job.imgy = self.getxy(val)


			elif name == 'Status':
				job.status = val
				# Do not try to fetch things that are banned (e.g. removed)
				if val == 'Banned':
					job.dlstate=-2
			elif name in ['Approver', 'Id', 'Source', 'Uploader']:
				pass
			else:
				self.log.warning("Unknown item key-value:")
				self.log.warning("	'{}' -> '{}'".format(name, val))

	def getImageUrl(self, soup):
		img = soup.find('a', text='Original image')
		return img['href']

	def extractMeta(self, job, soup):
		sidebar = soup.find('div', class_='sidebar4').find('div', class_='sidebar3')

		tagsection = sidebar.find('ul', id='tag-sidebar')
		assert tagsection
		infosection = sidebar.find('div', id='stats')
		assert infosection
		self.extractTags(job, tagsection)
		self.extractInfo(job, infosection)
		imgurl = self.getImageUrl(soup)

		return imgurl



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


	def fetchImage(self, job, url, srcurl):
		url = urllib.parse.urljoin(srcurl, url)
		fname = url.split("/")[-1]


		cont = self.wg.getpage(url, addlHeaders={'Referer':srcurl})

		fpath = self.saveFile(fname, cont)
		self.log.info("Saved file to path: '%s'", fpath)

		job.filename = fname
		job.filepath = fpath
		job.dlstate  = 2
		db.session.commit()
		# print(fname)

	def processJob(self, job):
		pageurl = 'http://gelbooru.com/index.php?page=post&s=view&id={}'.format(job.postid)
		while 1:
			try:
				soup = self.wg.getSoup(pageurl)
				if 'You are viewing an advertisement' in soup.get_text():
					self.log.warning("Working around advertisement. Sleeping 10 seconds")
					time.sleep(13)
				else:
					break
			except urllib.error.URLError:
				job.dlstate=-1
				db.session.commit()
				return

		if 'Gelbooru - Image List' in soup.title.get_text():
			self.log.warn("Image has been removed.")
			job.dlstate=-4
			db.session.commit()
			return

		if 'This post was deleted. Reason: Duplicate of' in soup.get_text():
			self.log.warn("Image has been removed.")
			job.dlstate=-6
			db.session.commit()
			return

		# text = soup.get_text()
		# if 'You need a gold account to see this image.' in text:
		# 	job.dlstate=-3
		# 	db.session.commit()
		# 	return

		err = 0
		while err < 5:
			try:
				imgurl = self.extractMeta(job, soup)
				if imgurl:
					self.fetchImage(job, imgurl, pageurl)
				else:
					self.log.info("No image found for URL: '%s'", pageurl)
					job.dlstate=-5
				break
			except sqlalchemy.exc.IntegrityError:
				err += 1
				db.session.rollback()
			except urllib.error.URLError:
				job.dlstate=-8
				db.session.commit()



	def retreiveItem(self):
		job = self.get_job()
		if not job:
			return False

		self.processJob(job)
		return True





def run(indice):
	print("Runner {}!".format(indice))
	fetcher = GelbooruFetcher()
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

if __name__ == '__main__':

	import logSetup
	logSetup.initLogging()

	run(1)

