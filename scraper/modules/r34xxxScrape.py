
import datetime
import re
import time
import traceback
import urllib.error
import urllib.parse

import sqlalchemy.exc
import parsedatetime

import scraper.runstate
import scraper.database as db
import scraper.fetchBase

class R34xxxFetcher(scraper.fetchBase.AbstractFetcher):

	pluginkey         = 'Rule34.xxx'
	loggerpath        = "Main.Rule34-xxx"
	content_count_max = 2300000

	def __init__(self):
		super().__init__()

	def extractTags(self, job, tagsection):

		characterlis = tagsection.find_all('li', class_='tag-type-character')
		artistlis    = tagsection.find_all('li', class_='tag-type-artist')
		taglis       = tagsection.find_all('li', class_='tag-type-general')


		tags = []
		for tagli in taglis:
			tag = tagli.find('a').get_text()
			tags.append(tag)

		artists = []
		for artistli in artistlis:
			artist = artistli.find('a').get_text()
			artists.append(artist)

		characters = []
		for characterli in characterlis:
			character = characterli.find('a').get_text()
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
		found = re.search(r"\((\d+)x(\d+)\)", instr)
		x, y = found.groups()
		return x, y

	def extractInfo(self, job, infosection):
		imgurl = None
		for li in infosection.find_all("li"):
			rawt = li.get_text()
			name, val = rawt.split(":", 1)

			name = name.strip()
			val = val.strip()

			if name == 'Rating':
				pass
				# job.rating = val
			elif name == 'Favorites':
				pass
				# job.favorites = val
			elif name == 'Score':
				val = val.strip()
				val = val.split()[0]
				job.score = val
			elif name == 'Posted':
				cal = parsedatetime.Calendar()
				val = val.split("by")[0]
				tstruct, pstat = cal.parse(val)
				assert pstat == 1 or pstat == 2 or pstat == 3
				job.posted = datetime.datetime.fromtimestamp(time.mktime(tstruct))
			elif name == 'Size':
				if not '\n' in val:
					return False
				fsize, res = val.split("\n")
				fsize, res = fsize.strip(), res.strip()
				job.imgx, job.imgy = self.getxy(res)

				link = li.find("a")
				if link:
					imgurl = link['href']

			elif name == 'Status':
				pass
			elif name in ['Approver', 'Id', 'Source', 'Uploader']:
				pass
			else:
				self.log.warning("Unknown item key-value:")
				self.log.warning("	'%s' -> '%s'", name, val)
		return imgurl

	def extractMeta(self, job, soup):
		tagsection = soup.find('ul', id='tag-sidebar')
		assert tagsection
		infosection = soup.find('div', id='stats')
		assert infosection
		self.extractTags(job, tagsection)
		imgurl = self.extractInfo(job, infosection)
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
		pageurl = 'https://rule34.xxx/index.php?page=post&s=view&id={}'.format(job.postid)
		try:
			soup = self.wg.getSoup(pageurl)
		except urllib.error.URLError:
			job.state = 'error'
			job.err_str = 'failure fetching container page'
			db.session.commit()
			return

		text = soup.get_text()
		if 'You need a gold account to see this image.' in text:
			job.state = 'removed'
			job.err_str = 'requires account'
			db.session.commit()
			return
		if 'This post was deleted for the following reasons' in text:
			job.state = 'removed'
			job.err_str = 'post deleted'
			db.session.commit()
			return
		if 'Save this flash' in text:
			job.state = 'disabled'
			job.err_str = 'content is flash .swf'
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
				break
			except AssertionError:
				self.log.info("Assertion error?: '%s'", pageurl)
				traceback.print_exc()
				job.state = 'error'
				job.err_str = 'Assertion failure?'
				db.session.rollback()
				break

			except urllib.error.URLError:
				job.state = 'error'
				job.err_str = 'failure fetching actual image'
				db.session.commit()

			except sqlalchemy.exc.IntegrityError:
				err += 1
				db.session.rollback()



	def retreiveItem(self):
		job = self.get_job()
		if not job:
			return False

		self.processJob(job)
		return True





def run(indice):
	print("Runner {}!".format(indice))
	fetcher = R34xxxFetcher()
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


