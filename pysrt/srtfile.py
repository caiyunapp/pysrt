# -*- coding: utf-8 -*-
import os
import sys
import codecs
import chardet

try:
    from collections import UserList
except ImportError:
    from UserList import UserList

from itertools import chain
from functools import reduce
from collections import Counter
from copy import copy

from pysrt.srtexc import Error
from pysrt.srtitem import SubRipItem
from pysrt.compat import str


BOMS = ((codecs.BOM_UTF32_LE, 'utf_32_le'),
        (codecs.BOM_UTF32_BE, 'utf_32_be'),
        (codecs.BOM_UTF16_LE, 'utf_16_le'),
        (codecs.BOM_UTF16_BE, 'utf_16_be'),
        (codecs.BOM_UTF8, 'utf_8'))
CODECS_BOMS = dict((codec, str(bom, codec)) for bom, codec in BOMS)
BIGGER_BOM = max(len(bom) for bom, encoding in BOMS)


class SubRipFile(UserList, object):
    """
    SubRip file descriptor.

    Provide a pure Python mapping on all metadata.

    SubRipFile(items, eol, path, encoding)

    items -> list of SubRipItem. Default to [].
    eol -> str: end of line character. Default to linesep used in opened file
        if any else to os.linesep.
    path -> str: path where file will be saved. To open an existant file see
        SubRipFile.open.
    encoding -> str: encoding used at file save. Default to utf-8.
    """
    ERROR_PASS = 0
    ERROR_LOG = 1
    ERROR_RAISE = 2

    DEFAULT_ENCODING = 'utf_8'

    def __init__(self, items=None, eol=None, path=None, encoding='utf-8'):
        UserList.__init__(self, items or [])
        self._eol = eol
        self.path = path
        self.encoding = encoding
        self.lang_stat = {}
        self.langs = []

    def _get_eol(self):
        return self._eol or os.linesep

    def _set_eol(self, eol):
        self._eol = self._eol or eol

    eol = property(_get_eol, _set_eol)

    def slice(self, starts_before=None, starts_after=None, ends_before=None,
              ends_after=None):
        """
        slice([starts_before][, starts_after][, ends_before][, ends_after]) \
-> SubRipFile clone

        All arguments are optional, and should be coercible to SubRipTime
        object.

        It reduce the set of subtitles to those that match match given time
        constraints.

        The returned set is a clone, but still contains references to original
        subtitles. So if you shift this returned set, subs contained in the
        original SubRipFile instance will be altered too.

        Example:
            >>> subs.slice(ends_after={'seconds': 20}).shift(seconds=2)
        """
        clone = copy(self)

        if starts_before:
            clone.data = (i for i in clone.data if i.start < starts_before)
        if starts_after:
            clone.data = (i for i in clone.data if i.start > starts_after)
        if ends_before:
            clone.data = (i for i in clone.data if i.end < ends_before)
        if ends_after:
            clone.data = (i for i in clone.data if i.end > ends_after)

        clone.data = list(clone.data)
        return clone

    def at(self, timestamp=None, **kwargs):
        """
        at(timestamp) -> SubRipFile clone

        timestamp argument should be coercible to SubRipFile object.

        A specialization of slice. Return all subtiles visible at the
        timestamp mark.

        Example:
            >>> subs.at((0, 0, 20, 0)).shift(seconds=2)
            >>> subs.at(seconds=20).shift(seconds=2)
        """
        time = timestamp or kwargs
        return self.slice(starts_before=time, ends_after=time)

    def shift(self, *args, **kwargs):
        """shift(hours, minutes, seconds, milliseconds, ratio)

        Shift `start` and `end` attributes of each items of file either by
        applying a ratio or by adding an offset.

        `ratio` should be either an int or a float.
        Example to convert subtitles from 23.9 fps to 25 fps:
        >>> subs.shift(ratio=25/23.9)

        All "time" arguments are optional and have a default value of 0.
        Example to delay all subs from 2 seconds and half
        >>> subs.shift(seconds=2, milliseconds=500)
        """
        for item in self:
            item.shift(*args, **kwargs)

    def clean_indexes(self):
        """
        clean_indexes()

        Sort subs and reset their index attribute. Should be called after
        destructive operations like split or such.
        """
        self.sort()
        for index, item in enumerate(self):
            item.index = index + 1

    @property
    def text(self):
        return '\n'.join(i.text for i in self)

    @classmethod
    def open(cls, path='', encoding=None, error_handling=ERROR_PASS):
        """
        open([path, [encoding]])

        If you do not provide any encoding, it can be detected if the file
        contain a bit order mark, unless it is set to utf-8 as default.
        """
        source_file, encoding = cls._open_unicode_file(path, claimed_encoding=encoding)
        new_file = cls(path=path, encoding=encoding)
        new_file.read(source_file, error_handling=error_handling)
        source_file.close()
        return new_file

    @classmethod
    def auto_open(cls, path='', error_handling=ERROR_PASS):
        assert os.path.isfile(path)
        f = open(path, 'rb')
        content = f.read()
        det_ret = chardet.detect(content)
        content = content.decode(det_ret["encoding"], errors='replace')

        new_file = cls(path=path, encoding=det_ret["encoding"])
        new_file.read(content.splitlines(True), error_handling=error_handling)

        new_file.lang_stat = Counter(chain.from_iterable([i.lang_map.keys() for i in new_file]))
        new_file.langs = [key for key in new_file.lang_stat if new_file.lang_stat[key] > int(len(new_file)/10)]

        return new_file

    @classmethod
    def from_string(cls, source, **kwargs):
        """
        from_string(source, **kwargs) -> SubRipFile

        `source` -> a unicode instance or at least a str instance encoded with
        `sys.getdefaultencoding()`
        """
        error_handling = kwargs.pop('error_handling', None)
        new_file = cls(**kwargs)
        new_file.read(source.splitlines(True), error_handling=error_handling)
        return new_file

    def read(self, source_file, error_handling=ERROR_PASS):
        """
        read(source_file, [error_handling])

        This method parse subtitles contained in `source_file` and append them
        to the current instance.

        `source_file` -> Any iterable that yield unicode strings, like a file
            opened with `codecs.open()` or an array of unicode.
        """
        self.eol = self._guess_eol(source_file)
        self.extend(self.stream(source_file, error_handling=error_handling))
        return self

    @classmethod
    def stream(cls, source_file, error_handling=ERROR_PASS):
        """
        stream(source_file, [error_handling])

        This method yield SubRipItem instances a soon as they have been parsed
        without storing them. It is a kind of SAX parser for .srt files.

        `source_file` -> Any iterable that yield unicode strings, like a file
            opened with `codecs.open()` or an array of unicode.

        Example:
            >>> import pysrt
            >>> import codecs
            >>> file = codecs.open('movie.srt', encoding='utf-8')
            >>> for sub in pysrt.stream(file):
            ...     sub.text += "\nHello !"
            ...     print unicode(sub)
        """
        string_buffer = []
        for index, line in enumerate(chain(source_file, '\n')):
            if line.strip():
                string_buffer.append(line)
            else:
                source = string_buffer
                string_buffer = []
                if source and all(source):
                    try:
                        yield SubRipItem.from_lines(source)
                    except Error as error:
                        error.args += (''.join(source), )
                        cls._handle_error(error, error_handling, index)

    def save(self, path=None, encoding=None, eol=None):
        """
        save([path][, encoding][, eol])

        Use initial path if no other provided.
        Use initial encoding if no other provided.
        Use initial eol if no other provided.
        """
        path = path or self.path
        encoding = encoding or self.encoding

        save_file = codecs.open(path, 'w+', encoding=encoding)
        self.write_into(save_file, eol=eol)
        save_file.close()

    def write_into(self, output_file, eol=None):
        """
        write_into(output_file [, eol])

        Serialize current state into `output_file`.

        `output_file` -> Any instance that respond to `write()`, typically a
        file object
        """
        output_eol = eol or self.eol

        for item in self:
            string_repr = str(item)
            if output_eol != '\n':
                string_repr = string_repr.replace('\n', output_eol)
            output_file.write(string_repr)
            # Only add trailing eol if it's not already present.
            # It was kept in the SubRipItem's text before but it really
            # belongs here. Existing applications might give us subtitles
            # which already contain a trailing eol though.
            if not string_repr.endswith(2 * output_eol):
                output_file.write(output_eol)

    @classmethod
    def _guess_eol(cls, string_iterable):
        first_line = cls._get_first_line(string_iterable)
        for eol in ('\r\n', '\r', '\n'):
            if first_line.endswith(eol):
                return eol
        return os.linesep

    @classmethod
    def _get_first_line(cls, string_iterable):
        if hasattr(string_iterable, 'tell'):
            previous_position = string_iterable.tell()

        try:
            first_line = next(iter(string_iterable))
        except StopIteration:
            return ''
        if hasattr(string_iterable, 'seek'):
            string_iterable.seek(previous_position)

        return first_line

    @classmethod
    def _detect_encoding(cls, path):
        file_descriptor = open(path, 'rb')
        first_chars = file_descriptor.read(BIGGER_BOM)
        file_descriptor.close()

        for bom, encoding in BOMS:
            if first_chars.startswith(bom):
                return encoding

        # TODO: maybe a chardet integration
        return cls.DEFAULT_ENCODING

    @classmethod
    def _open_unicode_file(cls, path, claimed_encoding=None):
        encoding = claimed_encoding or cls._detect_encoding(path)
        source_file = codecs.open(path, 'rU', encoding=encoding)

        # get rid of BOM if any
        possible_bom = CODECS_BOMS.get(encoding, None)
        if possible_bom:
            file_bom = source_file.read(len(possible_bom))
            if not file_bom == possible_bom:
                source_file.seek(0)  # if not rewind
        return source_file, encoding

    @classmethod
    def _handle_error(cls, error, error_handling, index):
        if error_handling == cls.ERROR_RAISE:
            error.args = (index, ) + error.args
            raise error
        if error_handling == cls.ERROR_LOG:
            name = type(error).__name__
            sys.stderr.write('PySRT-%s(line %s): \n' % (name, index))
            sys.stderr.write(error.args[0].encode('ascii', 'replace'))
            sys.stderr.write('\n')


    def match(self, other):
        assert type(other) == SubRipFile

        self_num = len(self)
        other_num = len(other)

        i = 0
        j = 0

        ret = SubRipFile()

        while i < self_num and j < other_num:
            
            other_bag = []
            self_bag = []
            
            start_delta = self[i].start.ordinal - other[j].start.ordinal

            end_delta = self[i].end.ordinal - other[j].end.ordinal

            if abs(start_delta) <= 1000:
                # 1 开始的误差都在可接受范围内，只有在这个条件下会匹配字幕
                if abs(end_delta) <= 1000:
                    # 1.1 结束的误差都在可接受范围内，则说明他们是同一条字幕
                    # 增加一对匹配的字幕
                    l_map = reduce(merge_dict, [self[i].lang_map, other[j].lang_map])
                    # l_map.update(self.lang_map)
                    # l_map.update(other.lang_map)
                    ret.append(SubRipItem(start=self[i].start, end=self[i].end, lang_map=l_map))
                    i += 1
                    j += 1
                    continue
                elif end_delta > 1000:
                    # 1.2 other中多条字幕匹配self中的一条字幕
                    # 条件意义：当other中的字幕结束时间远小于
                    if j+1 < other_num and other[j+1].end.ordinal - self[i].end.ordinal > 1000:
                        # 1.2.1 如果other下一条字幕存在，且结束时间远大于当前字幕结束时间，不匹配跳过
                        j+=1; i+=1
                        continue
                    # 如果下一条字幕存在，且结束时间远小于当前字幕结束时间，则说明other中多条字幕匹配当前字幕
                    other_bag.append(other[j])
                    while j+1 < other_num and other[j+1].end.ordinal - self[i].end.ordinal < 1000:
                        # 获取到other中的多条字幕并且放入other_bag
                        j += 1
                        other_bag.append(other[j])

                    # 当other_bag中当前条
                    if abs(other[j].end.ordinal - self[i].end.ordinal) < 1000:
                        # temp = " ".join(other_bag)
                        # ret.append([self[i].start, self[i].end, self[i].text.replace("\n", " ").encode('utf8'), temp.replace("\n", " ").encode('utf8')])
                        l_map = reduce(merge_dict, [i.lang_map for i in other_bag])
                        ret.append(SubRipItem(start=self[i].start, end=self[i].end, lang_map=l_map))
                        # print en_str_parsed[i].text.replace("\n", " ").encode('utf8'),temp.replace("\n", " ").encode('utf8')
                    i += 1; j += 1
                elif end_delta < -1000:
                    # 1.3 self中的多条字幕匹配other中的一条字幕
                    if i+1 < self_num and self[i+1].end.ordinal - other[j].end.ordinal > 1000:
                        j+=1; i+=1
                        continue
                    self_bag.append(self[i])
                    while i+1 < self_num and self[i+1].end.ordinal - other[j].end.ordinal < 1000:
                        i += 1
                        self_bag.append(self[i])
                    if abs(self[i].end.ordinal - other[j].end.ordinal) < 1000:
                        # temp = " ".join(self_bag)
                        # ret.append([other[j].start, other[j].end,temp.replace("\n", " ").encode('utf8'), other[j].text.replace("\n", " ").encode('utf8')])
                        l_map = reduce(merge_dict, [i.lang_map for i in self_bag])
                        ret.append(SubRipItem(start=self[i].start, end=self[i].end, lang_map=l_map))
                        # print zh_str_parsed[j].text.replace("\n", " ").encode('utf8'),temp.replace("\n", " ").encode('utf-8')
                    i += 1
                    j += 1
            elif start_delta < -1000 :
                i += 1
            else :
                j += 1

        ret.lang_stat = Counter(chain.from_iterable([i.lang_map.keys() for i in ret]))
        ret.langs = [key for key in ret.lang_stat if ret.lang_stat[key] > int(len(ret)/10)]

        return ret

    def build_corpus(self, root='./'):
        langs = set(self.langs)

        output_map = {l:open('{}/{}.corpus'.format(root, l), 'a+') for l in langs}

        for i in self:
            if set(i.lang_map.keys()) == langs:
                for key in output_map:
                    output_map[key].write(i.lang_map.get(key, '').encode('utf8') + '\n')

        for l in output_map:
            output_map[l].close()


def merge_dict(d1, d2):
    ret = {}
    for key in set(list(d1.keys())+list(d2.keys())):
        if key in d1 and key in d2:
            ret[key] = d1[key] + d2[key]
        else:
            ret[key] = d1[key] if key in d1 else d2[key]
    return ret

testfile = "/Users/hyy/workflow/pysrt/Foxcatcher.2014.1080p.BluRay.x264-SPARKS.简体&英文.srt"