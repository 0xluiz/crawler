from django.shortcuts import render
from django.db import connection

def index(request):
    with connection.cursor() as cursor:
        # Total FQDNs Crawled
        cursor.execute("SELECT COUNT(*) FROM fqdns WHERE crawled = TRUE")
        total_fqdns_crawled = cursor.fetchone()[0]

        # Total URLs to Crawl
        cursor.execute("SELECT COUNT(*) FROM to_crawl WHERE crawled = FALSE")
        total_urls_to_crawl = cursor.fetchone()[0]

        # Most Common Words (top 5 across all crawled pages)
        cursor.execute("""
            SELECT word, SUM(freq) as total_freq FROM (
                SELECT word1 AS word, COUNT(*) as freq FROM words GROUP BY word1
                UNION ALL
                SELECT word2 AS word, COUNT(*) as freq FROM words GROUP BY word2
                UNION ALL
                SELECT word3 AS word, COUNT(*) as freq FROM words GROUP BY word3
                UNION ALL
                SELECT word4 AS word, COUNT(*) as freq FROM words GROUP BY word4
                UNION ALL
                SELECT word5 AS word, COUNT(*) as freq FROM words GROUP BY word5
            ) AS combined_words
            GROUP BY word
            ORDER BY total_freq DESC
            LIMIT 5
        """)
        most_common_words = cursor.fetchall()

        # Crawling Progress (percentage)
        cursor.execute("SELECT COUNT(*) FROM fqdns")
        total_fqdns = cursor.fetchone()[0]
        crawling_progress = (total_fqdns_crawled / total_fqdns) * 100 if total_fqdns > 0 else 0

    context = {
        'total_fqdns_crawled': total_fqdns_crawled,
        'total_urls_to_crawl': total_urls_to_crawl,
        'most_common_words': most_common_words,
        'crawling_progress': crawling_progress,
    }
    return render(request, 'stats/index.html', context)
