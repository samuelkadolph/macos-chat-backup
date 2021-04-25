#!/usr/bin/env python3

# backup.py
#
# usage: backup.py [-h] [-a] [-c DB] [-d DIR] [-f FMT] [-g] [-z FMT]
#
# Archive MacOS Messsages to a directory with git commits for each day
#
# optional arguments:
#   -h, --help             show this help message and exit
#   -a, --no-attachments   do not archive attachments
#   -c DB, --db DB         specify the database path
#   -d DIR, --dir DIR      specify the archive directory
#   -f FMT, --fmt FMT      specify the timestamp format
#   -g, --no-git           do not use git
#   -z FMT, --day-fmt FMT  specify the date format
#
# MIT License
#
# Copyright (c) 2021 Samuel Kadolph
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import argparse
import shutil
import sqlite3
import subprocess
import sys

from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

class Attachment:
  def __init__(self, id, shortname, filename):
    self.id = id
    self.shortname = shortname
    self.filename = filename

  def __repr__(self):
    return "Attachment(%s)" % self

  def __str__(self):
    return "[%s]" % (self.dst_name())

  def dst_name(self):
    return "%s-%s" % (self.id, self.shortname)

  def src_name(self):
    return Path(self.filename).expanduser()

class Chat:
  ALL = """SELECT chat.ROWID, handle.ROWID, handle.id
           FROM chat
           INNER JOIN chat_handle_join ON chat_handle_join.chat_id = chat.ROWID
           INNER JOIN handle ON handle.ROWID = chat_handle_join.handle_id
           ORDER BY chat.ROWID, handle.id"""

  @classmethod
  def all(cls, cursor):
    chats = defaultdict(dict)
    for row in cursor.execute(cls.ALL):
      handle_id = row[1]
      handle = row[2]
      chats[row[0]][handle_id] = handle

    return [cls._load(id, chats[id]) for id in chats]

  @classmethod
  def _load(cls, id, participants):
    return cls(id, participants)

  def __init__(self, id, participants):
    self.id = id
    self.participants = participants

  def __repr__(self):
    return "Chat(%s)" % self

  def __str__(self):
    return ", ".join(self.participants.values())

  def dir_name(self):
    return ",".join(self.participants.values())

class Message:
  EARLIEST = """SELECT date
                FROM message
                ORDER BY date
                LIMIT 1"""
  FOR_DAY = """SELECT message.ROWID, message.date, chat_message_join.chat_id, handle.id, message.destination_caller_id, message.is_from_me, message.text, attachment.ROWID, attachment.transfer_name, attachment.filename
               FROM message
               INNER JOIN chat_message_join ON chat_message_join.message_id = message.ROWID
               LEFT JOIN handle on handle.ROWID = message.handle_id
               LEFT JOIN message_attachment_join ON message_attachment_join.message_id = message.ROWID
               LEFT JOIN attachment ON attachment.ROWID = message_attachment_join.attachment_id AND attachment.transfer_state = 5 AND attachment.hide_attachment != 1
               WHERE message.date BETWEEN :start AND :finish
               ORDER BY message.date"""

  @classmethod
  def earliest_date(cls, cursor):
    cursor.execute(cls.EARLIEST)
    return cls._convert_from_timestamp(cursor.fetchone()[0]).date()

  @classmethod
  def for_day(cls, cursor, date):
    params = {}
    params["start"] = cls._convert_to_timestamp(datetime.combine(date, datetime.min.time()))
    params["finish"] = cls._convert_to_timestamp(datetime.combine(date, datetime.max.time()))

    raw_messages = defaultdict(list)
    for row in cursor.execute(cls.FOR_DAY, params):
      raw_messages[row[0]].append(row)

    return [Message._load(raw_messages[i]) for i in raw_messages]

  @staticmethod
  def _convert_from_timestamp(value):
    return datetime.fromtimestamp(value / 1000000000 + 978307200)

  @staticmethod
  def _convert_to_timestamp(datetime):
    return int((datetime.timestamp() - 978307200) * 1000000000)


  @classmethod
  def _load(cls, raw):
    timestamp = cls._convert_from_timestamp(raw[0][1])
    chat_id = raw[0][2]
    handle_id = raw[0][3]
    caller_id = raw[0][4]
    is_from_me = raw[0][5] == 1
    text = raw[0][6]
    attachments = []

    for row in raw:
      if row[7] is not None:
        attachments.append(Attachment(row[7], row[8], row[9]))

    return cls(timestamp, chat_id, handle_id, caller_id, is_from_me, text, attachments)

  def __init__(self, timestamp, chat_id, handle_id, caller_id, is_from_me, text, attachments):
    self.timestamp = timestamp
    self.chat_id = chat_id
    self.handle_id = handle_id
    self.caller_id = caller_id
    self.is_from_me = is_from_me
    self.text = text
    self.attachments = attachments

  def __repr__(self):
    return "Message(timestamp=%r, chat_id=%r, handle_id=%r, caller_id=%r, is_from_me=%r, text=%r, attachments=%r)" % (self.timestamp, self.chat_id, self.handle_id, self.caller_id, self.is_from_me, self.text, self.attachments)

  def render(self, format, max_sender_length):
    attachments = " ".join(map(str, self.attachments))
    text = "\n".ljust(max_sender_length + 23).join(self.text.splitlines())

    if self.attachments:
      text = text.replace(u"\ufffc", "")

      if text:
        text = " ".join([text, attachments])
      else:
        text = attachments

    return "%s %s: %s" % (self.timestamp.strftime(format), self.sender().rjust(max_sender_length), text)

  def sender(self):
    if self.is_from_me:
      return self.caller_id
    else:
      return self.handle_id

def fatal(message):
  print(message, file=sys.stderr)
  exit(1)

def git(dir, *args):
  proc = subprocess.run(["git"] + list(args), cwd=dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  output = proc.stdout.decode(sys.getdefaultencoding())
  return (proc.returncode, output)

formatter = lambda prog: argparse.HelpFormatter(prog,max_help_position=25)
parser = argparse.ArgumentParser(formatter_class=formatter, description="Archive MacOS Messsages to a directory with git commits for each day")
parser.add_argument("-a", "--no-attachments", action="store_true", help="do not archive attachments")
parser.add_argument("-c", "--db", type=str, help="specify the database path")
parser.add_argument("-d", "--dir", type=str, help="specify the archive directory")
parser.add_argument("-f", "--fmt", type=str, help="specify the timestamp format")
parser.add_argument("-g", "--no-git", action="store_true", help="do not use git")
parser.add_argument("-z", "--day-fmt", type=str, help="specify the date format", metavar="FMT")
args = parser.parse_args()

attachments = not args.no_attachments
db = Path(args.db) if args.db else Path.home() / "Library" / "Messages" / "chat.db"
path = Path(args.dir) if args.dir else Path.cwd() / "messages"
fmt = args.fmt if args.fmt else "%Y-%m-%d %H:%M:%S"
use_git = not args.no_git
day_fmt = args.day_fmt if args.day_fmt else "%Y-%m-%d"

lastrun = path / "lastrun"

if not db.exists():
  fatal(f"db path '{db}' does not exist")

if not path.exists():
  path.mkdir(parents=True)

db = sqlite3.connect(f"file:{db}", uri=True)
cursor = db.cursor()

if use_git and not (path / ".git").exists():
  git(path, "init")

if not lastrun.exists():
  lastrun_at = Message.earliest_date(cursor)
else:
  with open(lastrun) as f:
    lastrun_at = date.fromisoformat(f.read().rstrip())

chats = { c.id: c for c in Chat.all(cursor) }

for d in range(int((date.today() - lastrun_at).days)):
  day = lastrun_at + timedelta(days=d)
  day_str = day.strftime(day_fmt)

  messages_by_chat = defaultdict(list)
  for message in Message.for_day(cursor, day):
    messages_by_chat[message.chat_id].append(message)

  for chat_id in messages_by_chat:
    chat_path = path / chats[chat_id].dir_name()
    messages = messages_by_chat[chat_id]
    max_sender_length = max(len(m.sender()) for m in messages)
    messages_path = chat_path  / f"{day_str}.txt"

    if not chat_path.exists():
      chat_path.mkdir()

    with open(messages_path, "w") as f:
      for message in messages:
        f.write("%s\n" % message.render(fmt, max_sender_length))

        if attachments:
          for attachment in message.attachments:
            attachment_path = chat_path / attachment.dst_name()

            shutil.copyfile(attachment.src_name(), attachment_path)

            if use_git:
              git(path, "add", attachment_path.relative_to(path))

    if use_git:
      git(path, "add", messages_path.relative_to(path))

  with open(lastrun, mode="w") as f:
    f.write("%s\n" % (day + timedelta(days=1)).isoformat())

  if use_git:
    git(path, "add", "lastrun")
    git(path, "commit", "--allow-empty", "--message", f"Chat Archive for {day_str}")

if use_git and git(path, "remote"):
  git(path, "push")
