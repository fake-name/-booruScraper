
import traceback
import urllib.error
import urllib.parse
import re
import time
import datetime

import sqlalchemy.exc
import parsedatetime

import scraper.runstate
import scraper.database as db
import scraper.fetchBase

import WebRequest

class GelbooruFetcher(scraper.fetchBase.AbstractFetcher):

	pluginkey         = 'Gelbooru'
	loggerpath        = "Main.Gelbooru"

	def __init__(self):
		super().__init__()

		# db.session = db.Session()


	def get_content_count_max(self):
		soup = self.wg.getSoup('https://gelbooru.com/index.php?page=post&s=list&tags=all')

		thumbs = soup.find_all('span', class_='thumb')
		tids = [tmp.get("id", "").strip("s") for tmp in thumbs]
		tids = [int(tmp) for tmp in tids if tmp]
		maxid = max(tids)
		return maxid

	def extractTags(self, job, metasection):

		tag_items = metasection.find_all('li', {"class" : re.compile(r'.*tag-type.*')})


		tags = []
		artists = []
		characters = []


		for tagli in tag_items:
			itemtype = [tmp for tmp in tagli.get("class") if "tag-type-" in tmp][0]
			tag_type = itemtype.split("-")[-1]

			if tag_type == "artist":
				artist = tagli.find_all('a')[-1].get_text()
				artists.append(artist)

			elif tag_type == "character":
				character = tagli.find_all('a')[-1].get_text()
				characters.append(character)

			elif tag_type == "copyright":
				tag = tagli.find_all('a')[-1].get_text()
				tags.append("copyright " + tag)

			elif tag_type == "general":
				tag = tagli.find_all('a')[-1].get_text()
				tags.append(tag)
			else:
				self.log.error("Unknown tag type: %s", tag_type)


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
			if "tag-type-" in str(li.get("class")):
				continue

			rawt = li.get_text().strip()
			if not rawt:
				continue
			if not ":" in rawt:
				# print("rawt: '{}'".format(rawt))
				continue


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
				# print("itemdate", itemdate)
				tstruct, pstat = cal.parse(itemdate)
				# print("Ret: ", pstat, tstruct)
				assert pstat == 1 or pstat == 2 or pstat == 3
				job.posted = datetime.datetime.fromtimestamp(time.mktime(tstruct))
			elif name == 'Size':
				job.imgx, job.imgy = self.getxy(val)


			elif name == 'Status':
				job.status = val
				# Do not try to fetch things that are banned (e.g. removed)
				if val == 'Banned':
					job.state = 'removed'
					job.err_str = 'item banned'
					self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
					db.session.commit()
			elif name in ['Approver', 'Id', 'Source', 'Uploader']:
				pass
			else:
				self.log.warning("Unknown item key-value:")
				self.log.warning("	'{}' -> '{}'".format(name, val))

	def getImageUrl(self, soup):
		img = soup.find('a', text='Original image')
		return img['href']

	def extractMeta(self, job, soup):
		metasections = soup.find_all('div', id='searchTags')

		assert metasections
		assert len(metasections) == 1

		metasection = metasections[0]

		self.extractTags(job, metasection)
		self.extractInfo(job, metasection)
		imgurl = self.getImageUrl(soup)

		return imgurl



	def fetchImage(self, job, url, srcurl):
		url = urllib.parse.urljoin(srcurl, url)
		fname = url.split("/")[-1]


		cont = self.wg.getpage(url, addlHeaders={'Referer':srcurl})

		fpath = self.saveFileRow(job, fname, cont)
		self.log.info("Saved file to path: '%s'", fpath)

		job.filename = fname
		job.filepath = fpath
		job.state    = 'complete'
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
			except WebRequest.WebGetException:
				job.state = 'error'
				job.err_str = 'failure fetching container page'
				self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
				db.session.commit()
				return

		if 'Gelbooru - Image List' in soup.title.get_text():
			self.log.warning("Image has been removed.")
			job.state = 'removed'
			job.err_str = 'image has been removed'
			self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			db.session.commit()
			return

		if 'This post was deleted. Reason: Duplicate of' in soup.get_text():
			self.log.warning("Image has been removed.")
			job.state = 'removed'
			job.err_str = 'image has been removed because it was a duplicate'
			self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			db.session.commit()
			return


		err = 0
		while err < 5:
			try:
				imgurl = self.extractMeta(job, soup)

				if imgurl:
					self.fetchImage(job, imgurl, pageurl)
				else:
					self.log.info("No image found for URL: '%s'", pageurl)
					job.state = 'error'
					job.err_str = 'failed to find image!'
					self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
				break
			except sqlalchemy.exc.IntegrityError:
				err += 1
				db.session.rollback()
			except WebRequest.WebGetException:
				job.state = 'error'
				job.err_str = 'failure fetching actual image'
				self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
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

	# print("Max count: ", fetcher.get_content_count_max())


	thing = lambda x: x
	thing.postid = 4594444
	thing.tags = []
	thing.artist = []
	thing.character = []


	fetcher.processJob(thing)

	# remainingTasks = True
	# try:
	# 	while remainingTasks and scraper.runstate.run:
	# 		remainingTasks = fetcher.retreiveItem()
	# except KeyboardInterrupt:
	# 	return
	# except:
	# 	print("Unhandled exception!")
	# 	traceback.print_exc()
	# 	raise

if __name__ == '__main__':

	import util.logSetup
	util.logSetup.initLogging()

	run(1)

