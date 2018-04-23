
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

import WebRequest

class DanbooruFetcher(scraper.fetchBase.AbstractFetcher):

	pluginkey         = 'Danbooru'
	loggerpath        = "Main.Danbooru"

	def __init__(self):
		super().__init__()


	def get_content_count_max(self):
		soup = self.wg.getSoup('https://danbooru.donmai.us/')

		thumbs = soup.find_all('article', class_='post-preview')
		tids = [tmp.get("id", "").strip("post_") for tmp in thumbs]
		tids = [int(tmp) for tmp in tids if tmp]
		maxid = max(tids)

		return maxid


	def extractTags(self, job, tagsection):

		characterlis = tagsection.find_all('li', class_='category-4')
		artistlis    = tagsection.find_all('li', class_='category-1')
		taglis       = tagsection.find_all('li', class_='category-0')
		copyrlis     = tagsection.find_all('li', class_='category-3')

		tags = []
		for copyrli in copyrlis:
			tag = copyrli.find_all('a')[-1].get_text()
			tags.append("copyright " + tag)

		for tagli in taglis:
			tag = tagli.find('a', class_="search-tag").get_text()
			tags.append(tag)

		artists = []
		for artistli in artistlis:
			artist = artistli.find('a', class_="search-tag").get_text()
			artists.append(artist)

		characters = []
		for characterli in characterlis:
			character = characterli.find('a', class_="search-tag").get_text()
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
				job.rating = val
			elif name == 'Favorites':
				job.favorites = val
			elif name == 'Score':
				job.score = val
			elif name == 'Date':
				cal = parsedatetime.Calendar()
				tstruct, pstat = cal.parse(val)
				assert pstat == 1 or pstat == 2
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
				job.status = val
				# Do not try to fetch things that are banned (e.g. removed)
				if val == 'Banned':
					job.state = 'removed'
					job.err_str = 'item banned'
					self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			elif name in ['Approver', 'ID', 'Source', 'Uploader']:
				pass
			else:
				self.log.warning("Unknown item key-value:")
				self.log.warning("	'%s' -> '%s'", name, val)
		return imgurl

	def extractMeta(self, job, soup):
		tagsection = soup.find('section', id='tag-list')
		assert tagsection
		infosection = soup.find('section', id='post-information')
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
		pageurl = 'https://danbooru.donmai.us/posts/{}'.format(job.postid)
		try:
			soup = self.wg.getSoup(pageurl)
		except WebRequest.WebGetException:
			job.state = 'error'
			job.err_str = 'failure fetching container page'
			self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			db.session.commit()
			return

		text = soup.get_text()
		if 'You need a gold account to see this image.' in text:
			job.state = 'removed'
			job.err_str = 'requires account'
			self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			db.session.commit()
			return
		if 'This post was deleted for the following reasons' in text:
			job.state = 'removed'
			job.err_str = 'post deleted'
			self.log.warning("Marking %s as %s (%s)", job.id, job.state, job.err_str)
			db.session.commit()
			return
		if 'Save this flash' in text:
			job.state = 'disabled'
			job.err_str = 'content is flash .swf'
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
					db.session.commit()

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
	fetcher = DanbooruFetcher()
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

