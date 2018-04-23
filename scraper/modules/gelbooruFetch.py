
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

	def extractTags(self, job, tagsection):

		characterlis = tagsection.find_all('li', class_='tag-type-character')
		specieslis   = tagsection.find_all('li', class_='tag-type-species')
		copyrlis     = tagsection.find_all('li', class_='tag-type-copyright')
		artistlis    = tagsection.find_all('li', class_='tag-type-artist')
		taglis       = tagsection.find_all('li', class_='tag-type-general')


		tags = []
		for tagli in taglis:
			tag = tagli.find_all('a')[-1].get_text()
			tags.append(tag)

		for speciesli in specieslis:
			tag = speciesli.find_all('a')[-1].get_text()
			tags.append("species " + tag)

		for copyrli in copyrlis:
			tag = copyrli.find_all('a')[-1].get_text()
			tags.append("copyright " + tag)

		artists = []
		for artistli in artistlis:
			artist = artistli.find_all('a')[-1].get_text()
			artists.append(artist)

		characters = []
		for characterli in characterlis:
			character = characterli.find_all('a')[-1].get_text()
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
		tagsection = soup.find('ul', id='tag-sidebar')
		assert tagsection
		infosection = soup.find('div', id='stats')
		assert infosection
		self.extractTags(job, tagsection)
		self.extractInfo(job, infosection)
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

if __name__ == '__main__':

	import logSetup
	logSetup.initLogging()

	run(1)

