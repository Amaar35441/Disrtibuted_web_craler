from crawler import WebCrawler

def main():
    # Seed URLs for crawling
    seed_urls = [
        'https://project-igi.en.softonic.com',
        'https://oceansofgamess.com'
    ]

    # Initialize and start crawler
    crawler = WebCrawler(seed_urls, max_depth=2, num_threads=3)
    crawler.start_crawling()

if __name__ == '__main__':
    main()