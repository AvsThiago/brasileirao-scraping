import bs4 as bs
import logging
import sys
import os
from lxml import etree
import re
from typing import IO
import asyncio
import aiofiles
import aiohttp
from aiohttp import ClientSession
import time

logging.basicConfig(
    format="%(message)s",
    level=logging.DEBUG,
    stream=sys.stderr,
)
logger = logging.getLogger("brasileirao_asyncio")
logging.getLogger("chardet.charsetprober").disabled = True

def extract_header_round(id_header: str, page_parsed) -> bs.element.Tag:
    return page_parsed.find(attrs={"id": id_header}).parent

def extract_round_header_text(header_round: bs.element.Tag) -> str:
    spans = header_round.find_all('span')
    # to deal with accentuation issues
    if len(spans) == 5:
        return spans[0].text
    return spans[1].text 

def extract_team_data(team: bs.element.Tag, st_home: bool) -> dict:
    home_or_visitor = 'HOME' if st_home else 'VISITOR'
    team_data = {
        "team_wiki_"  + home_or_visitor: team.find_all('a')[not st_home].get('href'),
        "team_name_"  + home_or_visitor: team.find_all('a')[not st_home].get('title'),
        "team_nick_"  + home_or_visitor: team.find_all('a')[not st_home].text,
        "team_state_" + home_or_visitor: team.find_all('a')[st_home].get('title').replace(' (estado)', '')
    }
    return team_data

def extract_stadium_data(stadium: bs.element.Tag) -> dict:
    stadium_data = {
        "stadium_name": stadium.find('a').get("title"),
        "stadium_nick": stadium.find('a').text,
        "stadium_city": stadium.find_all('a')[1].text
    }
    return stadium_data

def process_match_header(header: bs.element.Tag) -> dict:
    day_month, home, result, visitor, stadium = header.find_all('td')

    day_month = {"day_month": day_month.text.strip()}
    home = extract_team_data(team=home, st_home=True)
    result = {"result" : result.text.strip()}
    visitor = extract_team_data(team=visitor, st_home=False)
    stadium = extract_stadium_data(stadium=stadium)
    
    return {**day_month, **home, **result, **visitor, **stadium}

def extract_goals_time(goals: bs.element.Tag, dict_name: str) -> dict:
    goals_time = {
        dict_name: str([i.text.replace("'", "").strip() for i in goals.find_all('span')])
    }
    return goals_time

def extract_more_info(more_info: bs.element.Tag) -> dict:
    all_elements = more_info.find_all('b')
    audience = [i for i in all_elements if i.text == 'Público:']
    audience = audience[0].nextSibling.strip().replace(' ', '').replace('.','') if audience else ""
    income = [i for i in all_elements if i.text == 'Renda:']
    income = income[0].nextSibling.strip().replace(',', '.') if income else ""

    info = {
        "audience": audience,
        "income": income
    }
    return info

def process_match_details(details: bs.element.Tag) -> dict:
    hour, goals_home, _, goals_visitor, more_info = details.find_all('td')
    hour = {"hour": hour.text.replace('h',':').strip()}
    goals_home_team = extract_goals_time(goals_home, "goals_home_team")
    goals_visitor_team =  extract_goals_time(goals_visitor, "goals_visitor_team")
    more_info = extract_more_info(more_info)

    return {**hour, **goals_home_team, **goals_visitor_team, **more_info }

def extract_match_data(tb_match: bs.element.Tag) -> dict:
    rows = tb_match.find_all('tr')
    if not rows:
        return {}

    header, details = rows[:2]
    
    header = process_match_header(header=header)
    details = process_match_details(details=details)
    return {**header, **details}

async def parse_page(url: str, session: ClientSession, **kwargs) -> bs.element.Tag:
    try:
        page = await session.request(method="GET", url=url, raise_for_status=True, **kwargs)
        page_text = await page.text()
        parsed_page = bs.BeautifulSoup(page_text ,'lxml')
    except (
        aiohttp.ClientConnectionError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return None
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return None
    else:
        return parsed_page
    

async def extract_save_data_url(out_file: IO, url: str, session: ClientSession, **kwargs) -> None:
    parsed_page = await parse_page(url, session)
    page_description = parsed_page.find(attrs={"id": "firstHeading"}).text
    year = re.search(r'\d{4}', page_description).group()

    all_rounds = [i for i in parsed_page.find_all('span', {"class": "mw-headline"}) if (i.parent.name== "h3" or i.parent.name== "h2") and 'rodada' in str.lower(i.text)]
    header_round = extract_header_round(all_rounds[0].get('id'), parsed_page)

    previous = header_round
    current = header_round.find_next_sibling()

    async with aiofiles.open(out_file, 'a') as file:
        while 'Ver também' not in current.text:
            if current.name == 'h3':
                header_round = current
            elif previous.name != 'table':
                current_data = extract_match_data(current)
                if current_data:
                    line = ",".join([year, extract_round_header_text(header_round)] + list(current_data.values()))
                    await file.write(line.replace('[','"[').replace(']', ']"')+'\n')        
                    logger.info(", ".join([current_data['day_month'], 
                                           year, 
                                           extract_round_header_text(header_round),
                                           current_data['team_nick_HOME'], 
                                           current_data['result'],
                                           current_data['team_nick_VISITOR']]))
            previous = current
            current = current.find_next_sibling()
            
async def manage_crawl_and_write(out_file: IO, urls: list, **kwargs) -> None:
    async with ClientSession() as session:
        tasks = []
        for u in urls:
            tasks.append(
                extract_save_data_url(out_file=out_file, 
                                      url=u, 
                                      session=session, 
                                      **kwargs)
            )
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    out_file = "brazilian_chanpionship.csv"
    
    with open('urls.txt', 'r') as infile:
        urls = [i.strip() for i in infile.readlines()]
    
    OUT_HEADER = ['YEAR', 'ROUND', 'day_month', 'team_wiki_HOME', 'team_name_HOME', 'team_nick_HOME', 
            'team_state_HOME', 'result', 'team_wiki_VISITOR', 'team_name_VISITOR', 'team_nick_VISITOR',
            'team_state_VISITOR', 'stadium_name', 'stadium_nick', 'stadium_city', 'hour', 
            'goals_home_time', 'goals_visitor_time', 'audience', 'income']

    with open(out_file, 'w') as file:
        file.write(",".join(OUT_HEADER)+"\n")
    
    start = time.time()
    asyncio.run(manage_crawl_and_write(out_file=out_file, urls=urls))
    end = time.time()
    total = end-start
    print(f"\n\nRUNNING TIME: {round(total, 2)} seconds\n")
