#!/usr/bin/env python3
import psycopg2
import aiotg
import asyncio
import logging
import os

bot = aiotg.Bot(api_token=os.environ['API_TOKEN'])

def finish_markov(chat, cur):
    s = ''
    while True:
        a, b, c, _ = cur.fetchone()
        s += b + ' '
        if c == '':
            break
        cur.execute('''SELECT a, b, c, count FROM markov 
                           WHERE a=%s AND b=%s AND chat=%s
                           ORDER BY RANDOM()*count 
                           LIMIT 1''', [b, c, chat.id])
    return s

@bot.command(r'/markovski(?:@markovskibot)?$')
async def on_markov(chat, match):
    cur = conn.cursor()
    cur.execute('''SELECT a, b, c, count FROM markov WHERE a=\'\' AND chat=%s
                       ORDER BY RANDOM()*count LIMIT 1''', (chat.id,))
    s = finish_markov(chat, cur)
    await chat.send_text(s)

@bot.command(r'/markovski(?:@markovskibot)? (.*)')
async def on_markov_param(chat, match):
    cur = conn.cursor()
    param = match.group(1).lower()
    cur.execute('''SELECT a, b, c, count FROM markov
                       WHERE a=\'\' AND b=%s AND chat=%s
                       ORDER BY RANDOM()*count LIMIT 1''',
                (param, chat.id))
    s = finish_markov(chat, cur)
    await chat.send_text(s)


@bot.command(r'(?s)(.*)')
async def on_message(chat, match):
    message = match.group(0).lower()
    if message[0] == '/':
        logging.info('not adding command to chain')
        return

    ss = message.split()
    split = ['']*len(ss)
    for i, s in enumerate(ss):
        while s[0] == '@':
            s = s[1:]
        split[i] = s

    a = ['']*2 + split
    b = [''] + split + ['']
    c = split + ['']*2
    triplets = list(zip(a, b, c))[1:-1]

    cur = conn.cursor()
    print(chat.sender, triplets)
    for triplet in triplets:
        cur.execute('''DO $$BEGIN
                           UPDATE markov SET count=count+1 
                               WHERE a=%s AND b=%s AND c=%s AND chat=%s;
                           IF NOT FOUND THEN
                               INSERT INTO markov (a, b, c, chat, count) 
                                   VALUES (%s, %s, %s, %s, 1);
                           END IF;
                       END$$''', (triplet + (chat.id,)) * 2)
    conn.commit()


async def main():
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS markov
                       (id BIGSERIAL PRIMARY KEY, chat BIGSERIAL,
                        a TEXT, b TEXT, c TEXT, count INTEGER);''')
    conn.commit()

    await bot.loop()

if __name__ == '__main__':
    DB_NAME = os.environ['DB_NAME']
    DB_USER = os.environ['DB_USER']
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER)

    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        bot.stop()
