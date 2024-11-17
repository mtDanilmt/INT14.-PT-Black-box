import unittest
from unittest.mock import patch, MagicMock, Mock
import sqlite3
from io import StringIO
import sys
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
import urllib.error
from queue import Empty
from typing import Set
from main import (
    WikiLinkParser,
    fetch_page,
    parse_links,
    initialize_database,
    Crawler,
    display_help,
    display_version,
    is_valid_wikipedia_url,
)


class TestWikiLinkParser(unittest.TestCase):
    def test_is_wiki_article(self):
        parser = WikiLinkParser(base_url='https://ru.wikipedia.org')
        self.assertTrue(parser.is_wiki_article('/wiki/Example'))
        self.assertFalse(parser.is_wiki_article('/wiki/Special:Random'))
        self.assertFalse(parser.is_wiki_article('/w/index.php?title=Page'))
        self.assertFalse(parser.is_wiki_article('#fragment'))

    def test_handle_starttag(self):
        parser = WikiLinkParser(base_url='https://ru.wikipedia.org')
        parser.handle_starttag('a', [('href', '/wiki/Example')])
        self.assertIn('https://ru.wikipedia.org/wiki/Example', parser.links)


class TestFetchPage(unittest.TestCase):
    @patch('urllib.request.urlopen')
    def test_fetch_page_success(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b'<html></html>'
        mock_response.headers.get_content_charset.return_value = 'utf-8'

        # Настраиваем контекстный менеджер
        mock_urlopen.return_value.__enter__.return_value = mock_response

        html = fetch_page('http://example.com')
        self.assertEqual(html, '<html></html>')

    @patch('urllib.request.urlopen')
    def test_fetch_page_http_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url='http://example.com',
            code=404,
            msg='Not Found',
            hdrs=None,
            fp=None
        )
        with self.assertRaises(urllib.error.HTTPError):
            fetch_page('http://example.com')


class TestParseLinks(unittest.TestCase):
    def test_parse_links(self):
        html_content = '''
            <html>
                <body>
                    <a href="/wiki/Article1">Article1</a>
                    <a href="/wiki/Special:Random">Random</a>
                    <a href="/wiki/Article2">Article2</a>
                </body>
            </html>
        '''
        links = parse_links(html_content, 'https://ru.wikipedia.org')
        expected_links = {
            'https://ru.wikipedia.org/wiki/Article1',
            'https://ru.wikipedia.org/wiki/Article2'
        }
        self.assertEqual(links, expected_links)


class TestDatabaseInitialization(unittest.TestCase):
    def test_initialize_database(self):
        conn = initialize_database(':memory:')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = set(name for (name,) in cursor.fetchall())
        expected_tables = {'links', 'visited'}
        self.assertEqual(tables, expected_tables)
        conn.close()


class TestCrawler(unittest.TestCase):
    class TestCrawler(Crawler):
        def worker(self) -> None:
            while not self.stop_event.is_set():
                try:
                    current_url, depth = self.queue.get(timeout=1)
                except Empty:
                    # В тестах завершаем поток, когда очередь пуста
                    break
                print(f'Обработка: {current_url} на глубине {depth}')
                try:
                    html_content: str = fetch_page(current_url)
                    links: Set[str] = parse_links(html_content, current_url)
                    self.save_links(links)
                    self.mark_visited(current_url, depth)
                    for link in links:
                        if link not in self.visited and depth + 1 <= self.max_depth:
                            self.queue.put((link, depth + 1))
                    with self.db_lock:
                        self.total_links_processed += 1
                    print(f'Ссылок обработано: {self.total_links_processed}')
                except Exception as e:
                    print(f'Ошибка при обработке {current_url}: {e}')
                finally:
                    self.queue.task_done()

    @patch('main.fetch_page')
    @patch('main.parse_links')
    def test_worker(self, mock_parse_links, mock_fetch_page):
        mock_fetch_page.return_value = '<html></html>'
        mock_parse_links.return_value = set(['https://ru.wikipedia.org/wiki/Test'])

        crawler = self.TestCrawler('https://ru.wikipedia.org/wiki/Example', max_depth=1, num_threads=1)
        crawler.worker()
        self.assertTrue(crawler.is_visited('https://ru.wikipedia.org/wiki/Example'))
        self.assertTrue(crawler.is_visited('https://ru.wikipedia.org/wiki/Test'))
        crawler.conn.close()

    @patch.object(Crawler, 'worker')
    def test_crawler_initialization(self, mock_worker):
        crawler = Crawler('https://ru.wikipedia.org/wiki/Example', max_depth=1, num_threads=2)
        self.assertEqual(crawler.start_url, 'https://ru.wikipedia.org/wiki/Example')
        self.assertEqual(crawler.max_depth, 1)
        self.assertEqual(crawler.num_threads, 2)
        self.assertFalse(crawler.queue.empty())
        self.assertIn(('https://ru.wikipedia.org/wiki/Example', 0), crawler.queue.queue)
        crawler.conn.close()


class TestCLI(unittest.TestCase):
    @patch('sys.stdout', new_callable=StringIO)
    def test_display_help(self, mock_stdout):
        display_help()
        self.assertIn('CLI утилита для парсинга статей Википедии', mock_stdout.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_display_version(self, mock_stdout):
        display_version()
        self.assertIn('wiki webscraping 1.2.0', mock_stdout.getvalue())

    def test_is_valid_wikipedia_url(self):
        self.assertTrue(is_valid_wikipedia_url('https://ru.wikipedia.org/wiki/Example'))
        self.assertFalse(is_valid_wikipedia_url('https://google.com'))
        self.assertFalse(is_valid_wikipedia_url('ftp://ru.wikipedia.org/wiki/Example'))


if __name__ == '__main__':
    unittest.main()
