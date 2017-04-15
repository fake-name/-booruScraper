
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

import fetchBase

import fetchBase
class E621Fetcher(fetchBase.AbstractFetcher):

	pluginkey = 'e621'
	loggerpath = "Main.e621"

	def __init__(self):
		self.log = logging.getLogger("Main.e621")
		self.wg = webFunctions.WebGetRobust(logPath="Main.e621.Web")

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
				job.score = val
			elif name == 'Posted':
				cal = parsedatetime.Calendar()
				val = val.split("by")[0]
				tstruct, pstat = cal.parse(val)
				assert pstat == 1 or pstat == 2 or pstat == 3
				job.posted = datetime.datetime.fromtimestamp(time.mktime(tstruct))
			elif name == 'Size':
				res, fsize = val.split("(")
				fsize, res = fsize.strip(), res.replace("(", " ").replace(")", " ").strip()
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

		return imgurl

	def extractMeta(self, job, soup):
		tagsection = soup.find('ul', id='tag-sidebar')
		assert tagsection
		infosection = soup.find('div', id='stats')
		assert infosection
		imgurl = self.extractInfo(job, infosection)
		self.extractTags(job, tagsection)
		return imgurl



	def fetchImage(self, job, url, srcurl):
		url = urllib.parse.urljoin(srcurl, url)
		fname = url.split("/")[-1]


		cont = self.wg.getpage(url, addlHeaders={'Referer':srcurl})

		fpath = self.saveFile(job, fname, cont)
		self.log.info("Saved file to path: '%s'", fpath)

		job.filename = fname
		job.filepath = fpath
		job.dlstate  = 2
		db.session.commit()
		# print(fname)

	def processJob(self, job):
		pageurl = 'https://e621.net/post/show/{}'.format(job.postid)
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
	fetcher = E621Fetcher()
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





def test():
	fetcher = E621Fetcher()
	soup = fetcher.wg.getSoup("https://e621.net/post/show/1190129")
	fetcher.extractMeta(None, soup)



if __name__ == '__main__':

	import logSetup
	logSetup.initLogging()

	# test()
	run(1)


