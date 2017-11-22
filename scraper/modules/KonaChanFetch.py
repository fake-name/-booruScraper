
import datetime
import re
import time
import traceback
import urllib.error
import urllib.parse

import parsedatetime
import sqlalchemy.exc

import scraper.runstate
import scraper.database as db
import scraper.fetchBase

class KonaChanFetcher(scraper.fetchBase.AbstractFetcher):

	pluginkey         = 'KonaChan'
	loggerpath        = "Main.KonaChan"
	content_count_max = 245000

	def __init__(self):
		super().__init__()

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

		# print("Meta:")
		# print("tags:", tags)
		# print("artists:", artists)
		# print("characters:", characters)

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
		imgurl = None
		for li in infosection.find_all("li"):
			rawt = li.get_text()
			if ":" in rawt:
				name, val = rawt.split(":", 1)
			elif "Posted" in rawt:
				name, val = rawt.split("Posted", 1)
				name = "Posted"
			else:
				print("Don't know how to parse!")
				print("Raw - '{}'".format(rawt))


			name = name.strip()
			val = val.strip()

			if name == 'Rating':
				pass
			elif name == 'Favorites':
				pass
			elif name == 'ID':
				pass
			elif name == 'Favorited by':
				pass
			elif name == 'Score':
				val = val.strip()
				val = val.split()[0]
				# print('score: ', val)
				job.score = val
			elif name == 'Posted':
				cal = parsedatetime.Calendar()
				val = val.split("by")[0]
				tstruct, pstat = cal.parse(val)
				# print("Posted; ", tstruct)
				assert pstat == 1 or pstat == 2 or pstat == 3
				job.posted = datetime.datetime.fromtimestamp(time.mktime(tstruct))
			elif name == 'Size':
				res = val
				res = res.replace("(", " ").replace(")", " ").strip()
				# print("Size: ", self.getxy(res), li.find("a"))
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
				self.log.warning("	'{}' -> '{}'".format(name, val))

	def extractImageUrl(self, job, sidebar):
		imgurl_a = sidebar.find("a", class_='original-file-unchanged')
		if imgurl_a:
			imgurl = imgurl_a['href']
			print("ImageURL:", (imgurl_a['href'], imgurl_a))

		return imgurl

	def extractMeta(self, job, soup):
		tagsection = soup.find('ul', id='tag-sidebar')
		assert tagsection
		infosection = soup.find('div', id='stats')
		assert infosection
		self.extractInfo(job, infosection)
		self.extractTags(job, tagsection)
		sidebar = soup.find('div', class_='sidebar')
		imgurl = self.extractImageUrl(job, sidebar)
		return imgurl



	def fetchImage(self, job, url, srcurl):
		url = urllib.parse.urljoin(srcurl, url)
		fname = url.split("/")[-1]


		cont = self.wg.getpage(url, addlHeaders={'Referer':srcurl})

		fpath = self.saveFileRow(job, fname, cont)
		self.log.info("Saved file to path: '%s'", fpath)

		job.dlstate  = 2
		db.session.commit()
		# print(fname)

	def processJob(self, job):
		pageurl = 'https://konachan.com/post/show/{}'.format(job.postid)
		try:
			soup = self.wg.getSoup(pageurl)
		except urllib.error.URLError:
			job.dlstate=-1
			db.session.commit()
			return

		text = soup.get_text()
		if 'You need a gold account to see this image.' in text:
			job.dlstate=-3
			db.session.commit()
			return
		if 'This post was deleted for the following reasons' in text:
			job.dlstate=-4
			db.session.commit()
			return
		if 'Save this flash' in text:
			job.dlstate=-9
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
					job.dlstate=-5
				break
			except AssertionError:
				self.log.info("Assertion error?: '%s'", pageurl)
				traceback.print_exc()
				job.dlstate=-50
				db.session.rollback()
				break

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
	fetcher = KonaChanFetcher()
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

