import requests
import json
from datetime import date
import os.path
import logging

# setup logging
logger = logging.getLogger('apple_rss_scraper')
hdlr = logging.FileHandler('./apple/apple_rss_scraper.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)


RSS_url = 'https://rss.itunes.apple.com/api/v1/{region}/{media}/{chart}/{genre}/{limit}/{explicit}.json'
# limit can be 10, 50, 100, 200 or all (usually 200 results)

PATH = './apple/{}/{}.json' # + folders by
TODAY = date.today().isoformat()

REGIONS = ["us", "gb", "vn", "mn", "za", "mz", "mr", "tw", "fm", "sg", "gw", "cn", "kg", "jp", "fj",
    "hk", "gm", "mx", "co", "mw", "ru", "ve", "kr", "la", "in", "lr", "ar", "sv", "br",
    "gt", "ec", "pe", "do", "hu", "cl", "tr", "ae", "th", "id", "pg", "my", "na", "ph",
    "pw", "sa", "ni", "py", "pk", "hn", "st", "pl", "jm", "sc", "eg", "kz", "uy", "mo",
    "ee", "lv", "kw", "hr", "il", "ua", "lk", "ro", "lt", "np", "pa", "md", "am", "mt", "cz",
    "jo", "bw", "bg", "ke", "lb", "mk", "qa", "mg", "cr", "sk", "ne", "sn", "si", "ml", "mu",
    "ai", "bs", "tn", "ug", "bb", "bm", "ag", "dm", "gd", "vg", "ky", "lc", "ms", "kn", "bn",
    "tc", "gy", "vc", "tt", "bo", "cy", "sr", "bz", "is", "bh", "it", "ye", "fr", "dz", "de",
    "ao", "ng", "om", "be", "sl", "fi", "az", "sb", "by", "at", "uz", "tm", "zw",
    "gr", "sz", "ie", "tj", "au", "td", "nz", "cg", "cv", "pt", "es", "al", "lu", "tz", "nl",
    "gh", "no", "bf", "dk", "kh", "ca", "bj", "se", "bt", "ch"]

# Regions that do not have apple music top songs, top albums charts or music videos.
REGIONS_WITHOUT_APPLE_MUSIC = ['mz', 'mr', 'mw', 'lr', 'na', 'pw', 'pk', 'st',
'jm', 'sc', 'uy', 'kw', 'hr', 'mk', 'qa', 'mg', 'sn', 'ml', 'bs', 'tn', 'lc', 'ms', 'bn', 'tc', 'gy',
'vc', 'sr', 'is', 'ye', 'dz', 'ao', 'sl', 'sb', 'td', 'cg', 'al', 'tz', 'bf', 'bj', 'bt']
REGIONS_WITHOUT_ITUNES_MUSIC = ['mr','cn','mw','kr','lr','pw','pk','st','jm','sc','uy','kw','hr','mk','mg',
'sn','ml','tn','lc','ms','tc','gy','vc','sr', 'is','ye','dz','ao','sl','sb','td','cg','al','tz','bj','bt']
REGIONS_WITHOUT_MUSIC_VIDEOS = ['mr','gw','cn','mw','kr','lr','pw','pk','st','jm','sc','uy','kw',
'hr','np','lb','mk','qa','mg','sn','ml','tn','lc','ms','tc','gy','vc','sr','is','ye','dz','ao','om',
'sl','sb','td','cg','al','tz','bj','bt']

REGIONS_WITH_APPLE_MUSIC = list(set(REGIONS).difference(REGIONS_WITHOUT_APPLE_MUSIC))
REGIONS_WITH_ITUNES_MUSIC_ALBUMS = list(set(REGIONS).difference(REGIONS_WITHOUT_ITUNES_MUSIC))
REGIONS_WITH_MUSIC_VIDEOS = list(set(REGIONS).difference(REGIONS_WITHOUT_MUSIC_VIDEOS))

MEDIA_CHARTS = [
    # ('apple-music', 'top-songs'),
    # ('apple-music', 'top-albums'),
    ('itunes-music', 'top-albums'),
    ('itunes-music', 'top-songs'),
    ('music-videos', 'top-music-videos')
]

# Can associate charts with Regions. Change the for loops to acccomodate
# MEDIA_CHARTS_REGIONS = [
#     ('apple-music', 'top-songs', REGIONS_WITH_APPLE_MUSIC),
#     ('apple-music', 'top-albums', REGIONS_WITH_APPLE_MUSIC),
#     ('itunes-music', 'top-albums', REGIONS_WITH_ITUNES_MUSIC_ALBUMS),
#     ('itunes-music', 'top-songs', REGIONS_WITH_ITUNES_MUSIC_ALBUMS),
#     ('music-videos', 'top-music-videos', REGIONS_WITH_MUSIC_VIDEOS)
# ]

retry_paths = [] # TODO: retry these paths. Recursively?

# NOTE: This doesn't work
# for media_chart_region in MEDIA_CHARTS_REGIONS:
    # for media_chart in media_chart_region:
    #     for region in media_chart[2]:

for media_chart in MEDIA_CHARTS:
    if media_chart[0] == 'apple-music':
        regions = REGIONS_WITH_APPLE_MUSIC
    elif media_chart[0] == 'itunes-music':
        regions = REGIONS_WITH_ITUNES_MUSIC_ALBUMS
    elif media_chart[0] == 'music-videos':
        regions = REGIONS_WITH_MUSIC_VIDEOS
    else:
        print('Media chart and corresponding regions do not exist. Skipping...')
        continue

    for region in regions:
        # Generate the file path
        folder_path = media_chart[0] + '-' + media_chart[1]
        file_path = media_chart[0] + '_' + media_chart[1] + '_' + region + '_' + TODAY
        total_path = PATH.format(folder_path, file_path)

        # Check if the file exists
        if not os.path.exists(total_path):
            # Get URL to download
            data_url = RSS_url.format(region=region, media=media_chart[0], chart=media_chart[1], genre='all', limit=200, explicit='explicit')
            r = requests.get(data_url)

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print('Error: {} for {}'.format(str(e), file_path))
                if e.response.status_code == 502:
                    print('Error 502. Server issue with {} {} chart for region {}'.format(media_chart[0], media_chart[1], region))
                    logger.warning('502: Could not download {} {} chart for region {}: {}'.format(media_chart[0], media_chart[1], region, data_url))
                    print('Will retry {} later'.format(data_url))
                    retry_paths.append(data_url)

                elif e.response.status_code == 404:
                    logger.warning('404: No {} {} chart for region {}: {}'.format(media_chart[0], media_chart[1], region, data_url))
                else:
                    print('Error: {} for {}'.format(str(e), file_path))

                continue
            except requests.exceptions.RequestException as e:
                print(str(e))
                continue


            # Create the file and write to it
            with open(PATH.format(folder_path, file_path), 'w') as f:
                f.write(r.text)
            print('{} {} in {} on {} saved'.format(media_chart[0], media_chart[1], region, TODAY))

        else:
            print('{} {} in {} file already exists! Moving on... '.format(media_chart[0], media_chart[1], region, TODAY))

logger.warning(str(retry_paths))
