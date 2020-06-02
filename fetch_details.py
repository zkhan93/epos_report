import os
import sys
import time
import json
import logging
import hashlib
from reprlib import repr
import requests
from lxml import etree
from logging.handlers import RotatingFileHandler


# setup logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handlers = [RotatingFileHandler(filename="ration_details.log", maxBytes=5*1024*1024),
			logging.StreamHandler(stream=sys.stdout)]
for handler in handlers:
	handler.setFormatter(formatter)
	logger.addHandler(handler)


FPS_ID = 123300100909
DIST_CODE = 233

def get_temp_file(url, data):
	md5 = hashlib.md5()
	md5.update((url + json.dumps(data, sort_keys=True)).encode())
	return md5.hexdigest()

def fetch_content(url, data, fresh=False):
	filename = os.path.join('data', get_temp_file(url, data))
	logging.info(f"temp file {filename}")
	content = ''
	if fresh or not os.path.exists(filename):
		logging.info(f"fetching {url} from network")
		try:
			res = requests.post(url, data=data)
		except Exception as ex:
			logging.exception(ex)
		else:
			content = res.text
			with open(filename, 'w') as f:
				f.write(content)
	if os.path.exists(filename):
		with open(filename, 'r') as f:
			content = f.read()
	return content


def get_sales_data(month, year, fresh=False):
	data = dict(dist_code=DIST_CODE, fps_id=FPS_ID, month=month, year=year)
	sales_html = fetch_content('http://epos.bihar.gov.in/fps_transactions.action', data, fresh)
	if not sales_html:
		raise Exception("No data received ")
	html = etree.HTML(sales_html)
	table = html.findall("body/table")
	if table is None:
		raise Exception("Table tag not found in HTML response")
	table = table[0]

	def get_header(table):
		rows = list(table.findall('thead/tr'))
		# 0 has a table title
		# 1 has headers but with some columnspan
		# 2 has columns spanned headers
		headers = []
		for col in rows[1].findall('th'):
			text = col.text and col.text.strip()
			colspan = col.get('colspan')
			if colspan:
				spanned_cols = list(rows[2].findall('th'))
				for scol in spanned_cols:
					headers.append(text + '(' + scol.text + ')')
			else:
				headers.append(text)
		return headers

	def get_content(headers, table):
		rows = table.findall('tbody/tr')
		content = [dict(zip(headers, [col.text for col in row])) for row in rows[1:]]
		return content
	
	headers = get_header(table)
	content = get_content(headers, table)
	return content


def get_rc_details(rc_number, month, year, fresh=False):
	data = dict(src_no=rc_number, month=5, year=2020)
	url = 'http://epos.bihar.gov.in/SRC_Trans_Details.jsp'
	rc_html = fetch_content(url, data, fresh)
	if not rc_html:
		raise Exception("No data received ")
	html = etree.HTML(rc_html)
	table = html.findall("body/table")
	if table is None:
		raise Exception("No table tag not found in HTML response")
	first_table = table[0]
	trs = list(first_table.findall('tr'))
	if not trs:
		raise Exception("No tr in table")

	headers = [th.text for th in trs[2].findall('th')]
	content = [dict(zip(headers, [td.text for td in tr.findall('td')])) for tr in trs[3:]]
	return content


def fetch_data(month, year):
	logging.info("SALES DATA")
	sales_data = get_sales_data(month, year)
	logging.info(sales_data[0])
	logging.info(len(sales_data))
	logging.info(repr(sales_data))

	logging.info("RC DETAILS DATA")
	for sd in sales_data:
		try:
			time.sleep(1)
			rc_detail = get_rc_details(sd.get('RC No'), month, year)
			logging.info(rc_detail[0])
			logging.info(len(rc_detail))
			logging.info(repr(rc_detail))
		except Exception as ex:
			logging.error(str(ex))

if __name__ == '__main__':
	fetch_data(5, 2020)
