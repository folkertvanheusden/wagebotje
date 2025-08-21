#! /usr/bin/env python3

import paho.mqtt.client as mqtt
import random
import sqlite3
import sys
import threading
import time

from configuration import *

db_file = 'wagebotje.db'

def learn(parts):
    con = sqlite3.connect(db_file)

    cur = con.cursor()
    try:
        cur.execute('PRAGMA journal_mode=wal')
        cur.execute('CREATE TABLE start(start text, count integer, primary key (start))')
        cur.execute('CREATE TABLE word1(word1 text, count integer, primary key (word1))')
        cur.execute('CREATE TABLE word2(word1 text, word2 text, count integer, primary key (word1, word2))')
        cur.execute('CREATE TABLE word3(word1 text, word2 text, word3 text, count integer, primary key (word1, word2, word3))')
        cur.execute('CREATE TABLE end(end text, count integer, primary key (end))')
    except:
        pass
    cur.close()
    con.commit()

    cur = con.cursor()
    first = True
    pwords = []
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

def generate_one_sentence(words):
    con = sqlite3.connect(db_file)
    try:
        # start of sentence
        cur = con.cursor()
        cur.execute('select start, -LOG(RANDOM() + 1) / count AS priority, count FROM start WHERE start=? ORDER BY priority LIMIT 1', (words[0],))
        start_word = cur.fetchone()
        if start_word == None:
            cur.execute('select start, -LOG(RANDOM() + 1) / count AS priority, count FROM start ORDER BY priority LIMIT 1')
            start_word = cur.fetchone()
        cur.close()
        # end of sentence
        cur = con.cursor()
        cur.execute('select end, -LOG(RANDOM() + 1) / count AS priority, count FROM end ORDER BY priority LIMIT 1')
        end_word = cur.fetchone()
        cur.close()

        target_length = random.randint(2, 10)
        max_length = random.randint(target_length, 25)
        output = [ (start_word[0], start_word[2]) ]
        while len(output) < max_length:
            cur = con.cursor()

            row = None
            if len(output) >= 3:
                cur.execute('select word3, -LOG(RANDOM() + 1) / count AS priority, count FROM word3 WHERE word1=? AND word2=? ORDER BY priority LIMIT 1', (output[-2][0], output[-1][0]))
                row = cur.fetchone()

            if row == None or len(output) >= 2:
                cur.execute('select word2, -LOG(RANDOM() + 1) / count AS priority, count FROM word2 WHERE word1=? ORDER BY priority LIMIT 1', (output[-1][0], ))
                row = cur.fetchone()

            if row == None:
                cur.execute('select word1, -LOG(RANDOM() + 1) / count AS priority, count FROM word1 ORDER BY priority LIMIT 1')
                row = cur.fetchone()

            cur.close()

            if row == None:
                continue

            output.append((row[0], row[2]))

            if len(output) >= target_length:
                cur = con.cursor()
                cur.execute('SELECT COUNT(*) AS n FROM end WHERE end=? LIMIT 1', (output[-1][0],))
                n_end = cur.fetchone()[0]
                if n_end > 0:
                    break

        reply = ' '.join([token[0] for token in output])
        reply_sum = sum([token[1] for token in output])

        con.close()
        return (reply, reply_sum)

    except Exception as e:
        con.close()
        print(f'Exception: {e}, line number: {e.__traceback__.tb_lineno}')
        raise e

def generate_reply(tokens):
    con = sqlite3.connect(db_file)

    lowest = (None, 100000000000000000000)

    choices = []
    pwords = []
    for word in tokens:
        pwords.append(word)
        while len(pwords) > 3:
            del pwords[0]

        # generate a sentence with each word, sum the counts for each word in such a sentence, do a weighted random selection of those sentences
        rc = generate_one_sentence(pwords)
        if not rc[0] is None:
            choices.append(rc)
            if rc[1] < lowest[1]:
                lowest = rc

    con.close()

    return lowest[0]

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

    tokens = [ token.lower() for token in text.split(' ') ]
    learn(tokens)

    parts   = topic.split('/')
    channel = parts[2] if len(parts) >= 3 else 'nurdbottest'  # default channel if can't be deduced
    hostmask = parts[3] if len(parts) >= 4 else 'jemoeder'  # default nick if it can't be deduced
    nickname = hostmask.split('!')[0]

    message_response_topic = f'{topic_prefix}to/irc/{channel}/privmsg'
    karma_command_topic = f'{topic_prefix}from/irc/{channel}/{hostmask}/message'

    if channel in channels or (len(channel) >= 1 and channel[0] == '\\'):
        response_topic = f'{topic_prefix}to/irc/{channel}/notice'

        command = tokens[0]

        if command == 'wagebotje:':
            try:
                reply = generate_reply(tokens)
                if not reply is None:
                    client.publish(response_topic, reply)

            except Exception as e:
                client.publish(response_topic, f'Exception: {e}, line number: {e.__traceback__.tb_lineno}')


def on_connect(client, userdata, flags, rc):
    client.subscribe(f'{topic_prefix}from/irc/#')

if len(sys.argv) >= 2:
    for line in open(sys.argv[1], 'r').readlines():
        line = line.rstrip('\n')
        learn(line.split(' '))

else:
    client = mqtt.Client()
    client.on_message = on_message
    client.on_connect = on_connect
    client.connect(mqtt_server, port=mqtt_port, keepalive=4, bind_address='')

    client.loop_forever()
