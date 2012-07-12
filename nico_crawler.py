#!/usr/bin/env python
import sys
import xml.dom.minidom
import boto
import urllib
import random
import string
import time

conn = boto.connect_dynamodb()
nico_url_base = 'http://ext.nicovideo.jp/api/getthumbinfo/'

class NotFoundException(Exception): pass
class AlreadyRegisteredException(Exception): pass

def rand_video_id():
    return 'sm%s' % ''.join(random.choice(string.digits) for i in xrange(random.randint(1, 10)))

def register_video(video_id, video_title):
    video_table = conn.get_table('nicovideo_videos')
    item = boto.dynamodb.item.Item(video_table, video_id)
    item.put_attribute('title', video_title)
    ret = item.save(return_values='ALL_OLD')

    if ret.has_key('Attributes'):
        raise AlreadyRegisteredException('%s is already registered!' % video_id)

def add_count(video_tag):
    tag_table = conn.get_table('nicovideo_tags')
    item = boto.dynamodb.item.Item(tag_table, video_tag)
    item.add_attribute('count', 1)
    item.save()

def nico_get(video_id):
    url = nico_url_base + video_id
    f = urllib.urlopen(url)
    return f.read()

def niconico():
    video_id = rand_video_id()
    dom = xml.dom.minidom.parseString(nico_get(video_id))
    t = dom.getElementsByTagName('title')

    if not t:
        raise NotFoundException('video not found %s' % video_id)

    title = t[0].firstChild.data

    register_video(video_id, title)

    for url in dom.getElementsByTagName("tag"):
        video_tag = url.firstChild.data
        if video_tag:
            add_count(video_tag)

    time.sleep(0.3)

while True:
    try:
        niconico()
    except Exception, err:
        #print >> sys.stderr, err
        pass
