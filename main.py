import sys
import urllib.request
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
import sqlite3
import threading
import argparse
from typing import Set, List, Tuple, Optional
import time
from queue import Queue, Empty


class WikiLinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.links: Set[str] = set()
        self.base_url: str = base_url

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag == 'a':
            href_dict: dict[str, Optional[str]] = dict(attrs)
            href: Optional[str] = href_dict.get('href')
            if href and self.is_wiki_article(href):
                full_url: str = urljoin(self.base_url, href)
                self.links.add(full_url)

    def is_wiki_article(self, href: str) -> bool:
        parsed_href = urlparse(href)
        if parsed_href.fragment:
            return False
        if parsed_href.path.startswith('/wiki/') and not parsed_href.path.startswith('/wiki/Special:'):
            return True
        return False


def fetch_page(url: str) -> str:
    try:
        with urllib.request.urlopen(url) as response:
            content_type: Optional[str] = response.headers.get_content_charset()
            html_content: str = response.read().decode(content_type or 'utf-8')
        return html_content
    except urllib.error.HTTPError as e:
        print(f'HTTP ошибка при загрузке {url}: {e}')
        raise
    except urllib.error.URLError as e:
        print(f'URL ошибка при загрузке {url}: {e}')
        raise
    except Exception as e:
        print(f'Неизвестная ошибка при загрузке {url}: {e}')
        raise


def parse_links(html_content: str, base_url: str) -> Set[str]:
    parser: WikiLinkParser = WikiLinkParser(base_url)
    parser.feed(html_content)
    return parser.links


def initialize_database(db_name: str) -> sqlite3.Connection:
    conn: sqlite3.Connection = sqlite3.connect(db_name, check_same_thread=False)
    cursor: sqlite3.Cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links (
            url TEXT PRIMARY KEY
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visited (
            url TEXT PRIMARY KEY,
            depth INTEGER
        )
    ''')
    conn.commit()
    return conn


class Crawler:
    def __init__(self, start_url: str, max_depth: int, num_threads: int = 10) -> None:
        self.start_url: str = start_url
        self.max_depth: int = max_depth
        self.conn: sqlite3.Connection = initialize_database('wikipedia_links.db')
        self.db_lock: threading.Lock = threading.Lock()
        self.queue: Queue = Queue()
        self.queue.put((start_url, 0))
        self.visited: Set[str] = set()
        self.num_threads: int = num_threads
        self.total_links_processed: int = 0
        self.stop_event = threading.Event()

    def is_visited(self, url: str) -> bool:
        with self.db_lock:
            cursor: sqlite3.Cursor = self.conn.cursor()
            cursor.execute('SELECT 1 FROM visited WHERE url = ?', (url,))
            result = cursor.fetchone()
            return result is not None

    def mark_visited(self, url: str, depth: int) -> None:
        with self.db_lock:
            cursor: sqlite3.Cursor = self.conn.cursor()
            cursor.execute('INSERT OR IGNORE INTO visited (url, depth) VALUES (?, ?)', (url, depth))
            self.conn.commit()
            self.visited.add(url)

    def save_links(self, links: Set[str]) -> None:
        with self.db_lock:
            cursor: sqlite3.Cursor = self.conn.cursor()
            cursor.executemany('INSERT OR IGNORE INTO links (url) VALUES (?)', [(link,) for link in links])
            self.conn.commit()

    def worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                current_url, depth = self.queue.get(timeout=1)
            except Empty:
                continue
            if depth > self.max_depth:
                self.queue.task_done()
                continue
            if current_url in self.visited:
                self.queue.task_done()
                continue
            print(f'Обработка: {current_url} на глубине {depth}')
            try:
                html_content: str = fetch_page(current_url)
                links: Set[str] = parse_links(html_content, current_url)
                self.save_links(links)
                self.mark_visited(current_url, depth)
                for link in links:
                    if link not in self.visited:
                        self.queue.put((link, depth + 1))
                with self.db_lock:
                    self.total_links_processed += 1
                print(f'Ссылок обработано: {self.total_links_processed}')
            except urllib.error.HTTPError as e:
                print(f'HTTP ошибка при обработке {current_url}: {e}')
            except urllib.error.URLError as e:
                print(f'URL ошибка при обработке {current_url}: {e}')
            except UnicodeDecodeError as e:
                print(f'Ошибка декодирования при обработке {current_url}: {e}')
            except Exception as e:
                print(f'Неизвестная ошибка при обработке {current_url}: {e}')
            finally:
                self.queue.task_done()

    def crawl(self) -> None:
        start_time = time.time()
        threads: List[threading.Thread] = []
        try:
            for _ in range(self.num_threads):
                thread: threading.Thread = threading.Thread(target=self.worker)
                thread.start()
                threads.append(thread)
            # Ожидание завершения очереди
            while not self.queue.empty():
                time.sleep(0.1)
        except KeyboardInterrupt:
            print('Остановка выполнения по требованию пользователя...')
            self.stop_event.set()
        finally:
            # Дожидаемся завершения потоков
            for thread in threads:
                thread.join()
            end_time = time.time()
            print(f'Обход завершен за {end_time - start_time:.2f} секунд.')
            self.conn.close()


def display_help() -> None:
    help_text: str = """
CLI утилита для парсинга статей Википедии.

Использование:
    python script.py <URL статьи Википедии>

Команды:
    --help, -h, help, list all commands
        Выводит это сообщение помощи и описывает, как использовать утилиту.
    --version
        Выводит информацию о версии утилиты.

Описание:
    Утилита получает на вход ссылку на статью в Википедии, парсит страницу и ищет ссылки на другие статьи.
    Затем она рекурсивно обходит найденные статьи до глубины 6 и сохраняет все уникальные ссылки в базу данных SQLite.
"""
    print(help_text)


def display_version() -> None:
    version_text: str = "wiki webscraping 1.2.0"
    print(version_text)


def is_valid_wikipedia_url(url: str) -> bool:
    parsed_url = urlparse(url)
    if parsed_url.scheme not in {'http', 'https'}:
        return False
    if 'wikipedia.org' not in parsed_url.netloc:
        return False
    if not parsed_url.path.startswith('/wiki/'):
        return False
    return True


def main() -> None:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('url', nargs='?', help='URL статьи Википедии для начала парсинга')
    parser.add_argument('--help', '-h', action='store_true', help='Показать сообщение помощи и выйти')
    parser.add_argument('--version', action='store_true', help='Показать информацию о версии и выйти')
    args, unknown = parser.parse_known_args()

    # Проверка на дополнительные команды
    if args.help or (args.url and args.url.lower() in {'list'}):
        display_help()
        sys.exit(0)

    if args.version or (args.url and args.url.lower() == '--version'):
        display_version()
        sys.exit(0)

    if not args.url:
        print('Ошибка: не указан URL статьи Википедии.')
        print('Для помощи используйте: --help')
        sys.exit(1)

    start_url: str = args.url
    if not is_valid_wikipedia_url(start_url):
        print('Пожалуйста, укажите корректный URL статьи Википедии.')
        sys.exit(1)

    crawler: Crawler = Crawler(start_url, max_depth=6, num_threads=10)
    crawler.crawl()


if __name__ == '__main__':
    main()
