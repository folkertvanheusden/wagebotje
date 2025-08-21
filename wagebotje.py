#! /usr/bin/env python3

import paho.mqtt.client as mqtt
import random
import sqlite3
import sys
import threading
import time

from configuration import *

def learn(text):
    db_file = 'wagebotje.db'
    con = sqlite3.connect(db_file)

    cur = con.cursor()
    try:
        cur.execute('CREATE TABLE start(start text, count integer, primary key (start))')
        cur.execute('CREATE TABLE word1(word1 text, count integer, primary key (word1))')
        cur.execute('CREATE TABLE word2(word1 text, word2 text, count integer, primary key (word1, word2))')
        cur.execute('CREATE TABLE word3(word1 text, word2 text, word3 text, count integer, primary key (word1, word2, word3))')
        cur.execute('CREATE TABLE end(end text, count integer, primary key (end))')
    except:
        pass
    cur.close()

    cur = con.cursor()
    first = True
    pwords = []
    parts = text.split()
    for word in parts:
        if first:
            first = False
            if ':' in word:
                continue
            pwords.append(word)
            cur.execute('INSERT INTO start(start, count) VALUES(?, 1) ON CONFLICT (start) DO UPDATE SET count=count+1', (word,))
        else:
            pwords.append(word)
        while len(pwords) > 3:
            del pwords[0]

        if len(pwords) >= 1:
            cur.execute('INSERT INTO word1(word1, count) VALUES(?, 1) ON CONFLICT (word1) DO UPDATE SET count=count+1', (pwords[-1],))
        if len(pwords) >= 2:
            cur.execute('INSERT INTO word2(word1, word2, count) VALUES(?, ?, 1) ON CONFLICT (word1, word2) DO UPDATE SET count=count+1', (pwords[-2], pwords[-1]))
        if len(pwords) >= 3:
            cur.execute('INSERT INTO word3(word1, word2, word3, count) VALUES(?, ?, ?, 1) ON CONFLICT (word1, word2, word3) DO UPDATE SET count=count+1', (pwords[-3], pwords[-2], pwords[-1]))

    if len(parts) > 0:
        cur.execute('INSERT INTO end(end, count) VALUES(?, 1) ON CONFLICT (end) DO UPDATE SET count=count+1', (word,))

    cur.close()

    con.commit()
    con.close()

def on_message(client, userdata, message):
    global prefix

    text = message.payload.decode('utf-8')

    topic = message.topic[len(topic_prefix):]

    if topic == 'from/bot/command' and text == 'register':
        announce_commands(client)
        return

    if topic == 'from/bot/parameter/prefix':
        prefix = text
        return

    if len(text) == 0:
        return

    learn(text)

    parts   = topic.split('/')
    channel = parts[2] if len(parts) >= 3 else 'nurdbottest'  # default channel if can't be deduced
    hostmask = parts[3] if len(parts) >= 4 else 'jemoeder'  # default nick if it can't be deduced
    nickname = hostmask.split('!')[0]

    message_response_topic = f'{topic_prefix}to/irc/{channel}/privmsg'
    karma_command_topic = f'{topic_prefix}from/irc/{channel}/{hostmask}/message'

    if channel in channels or (len(channel) >= 1 and channel[0] == '\\'):
        response_topic = f'{topic_prefix}to/irc/{channel}/notice'

        tokens  = text.split(' ')
        command = tokens[0][1:]

        if command == 'wagebotje:':
            try:
                pass

            except Exception as e:
                client.publish(response_topic, f'Exception: {e}, line number: {e.__traceback__.tb_lineno}')


def on_connect(client, userdata, flags, rc):
    client.subscribe(f'{topic_prefix}from/irc/#')

client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect
client.connect(mqtt_server, port=mqtt_port, keepalive=4, bind_address='')

client.loop_forever()
