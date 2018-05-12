# -*- coding: utf-8 -*-
import scrapy
import re
from time import gmtime, strftime
import pandas as pd
import numpy as np
from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)+1):
        yield start_date + timedelta(n)

class StatisticsSpider(scrapy.Spider):
    name = 'Statistics'
#    start_urls = ['http://www.espn.com/soccer/fixtures/_/date/20180509/league/bra.copa_do_brazil']

    # Start and end craller 
    #start_date = date(2014, 1, 1)
    #end_date   = date(2017, 12, 31)

    start_date = date(2018, 1, 1)
    end_date   = date(2018, 12, 31)
    #league     = ['bra.1']
    league     = ['bra.1', 'bra.copa_do_brazil']

    def start_requests(self):
        url_base = 'http://www.espn.com/soccer/fixtures/_/date'
        
        for league in self.league:
          for single_date in daterange(self.start_date, self.end_date):
              url = '{}/{}/league/{}'.format(url_base, single_date.strftime("%Y%m%d"), league)
              yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
      for link in response.css("tr.has-results span.record a::attr(href)").extract():
        yield response.follow(link, self.parse_matchstats)

    def parse_matchstats(self, response):
      url    = response.url
      gameID = response.url.split("=")[-1]

      championship = response.css("div.game-details ::text").extract_first()

      team_home    = self.rename_key_by_team('home', self.parse_team(response, 'home'))
      team_away    = self.rename_key_by_team('away', self.parse_team(response, 'away'))

      team_home_last_games = self.rename_key_by_team('home',
                              self.parse_last_games(response, team_home['home_name'], 'teamFormHome'))

      team_away_last_games = self.rename_key_by_team('away',
                              self.parse_last_games(response, team_away['away_name'], 'teamFormAway'))


      item     = {}
      item.update({'url': url, 'gameID': gameID, 'championship': championship})
      item.update(team_home)
      item.update(team_home_last_games)
      item.update(team_away)
      item.update(team_away_last_games)


      request = scrapy.Request('http://www.espn.com/soccer/match?gameId='+gameID,
                                 callback=self.parse_match, meta={'item': item})

      yield request

    # http://www.espn.com/soccer/match?gameId=507254
    def parse_match(self, response):
      stadium = response.css("#gamepackage-game-information li.venue ::text").extract()[1]
      date    = response.css("#gamepackage-game-information").xpath('//span[@data-date]').css("::attr(data-date)").extract_first()
      adress  = response.css("#gamepackage-game-information .address span ::text").extract_first()

      # {**response.meta['item'], **{'stadium': stadium, 'date': date, 'adress': adress}}
      obj     = response.meta['item']
      obj.update({'stadium': stadium, 'date': date, 'adress': adress})
      yield obj

    def parse_team(self, response, team):
      # Fix. In some cases, the team is changed
      if team == 'home':
        team2 = 'away'
      else:
        team2 = 'home'

      name        = response.css("div.team.{} .short-name ::text".format(team2)).extract_first()
      score       = int(response.css("div.team.{} span.score ::text".format(team2)).extract_first())

      stats_label = response.css("div.stat-list").xpath('//td[@data-home-away="{}"]/@data-stat'.format(team)).extract()
      stats_value = response.css("div.stat-list").xpath('//td[@data-home-away="{}"]/text()'.format(team)).extract()
      possesion   = response.xpath('//span[@data-home-away="{}"]'.format(team)).css(".chartValue ::text").extract_first()

      shots = response.css("div.shots").xpath('//span[@data-home-away="{}"]'.format(team)).css(".number ::text").extract_first()
      if shots is None:
        shots = shots_goal = None
      else:
        shots, shots_goal, _ = re.split("\(|\)", shots)      

      values = {'name': name, 'score': score, 'possesion': possesion, 'shots': shots, 'shots_goal': shots_goal}
      stats  = dict(zip(stats_label, stats_value))
      
      obj    = values.copy()
      obj.update(stats)
      return obj


    def parse_last_games(self, response, team_name, tag):
      parse_result = {'L': -1, 'D': 0, 'W': 1}

      # teamFormHome
      # teamFormAway

      score_team = []
      # teamFormHome
      for tr in response.xpath('//div[@data-module="{}"]'.format(tag)).css(".content tr")[1:]:
        #result, home_team, home_team_slug, score, away_team, away_team_slug, _, _, date, _, _, competition = tr.css("::text").extract()
        #home_score, away_score = score.split("-")
        result = tr.css("td ::Text").extract_first()
        score_team.append(parse_result[result])

      # teamFormHeadToHead
      teams = {}
      for tr in response.xpath('//div[@data-module="teamFormHeadToHead"]').css(".content tr")[1:]:
        tr_data = tr.css("::text").extract()

        home_team = tr_data[3]
        away_team = tr_data[25]
        home_score, away_score = tr_data[15].split("-")

        if not home_team in teams:
          teams[home_team] = []

        if not away_team in teams:
          teams[away_team] = []

        if int(home_score) == int(away_score):
          teams[home_team].append(parse_result['D'])
          teams[away_team].append(parse_result['D'])
        else: 
          if int(home_score) < int(away_score):
            teams[home_team].append(parse_result['L'])
            teams[away_team].append(parse_result['W'])
          else:
            teams[home_team].append(parse_result['W'])
            teams[away_team].append(parse_result['L'])

      # Weights in time, recent games have priority
      # array([2.6, 2.2, 1.8, 1.4, 1. ])
      weights             = np.array([1+(i*2/5) for i in range(5)])[::-1]
      last_five_all_games = np.array(score_team).dot(weights[0:len(score_team)])
      
      if len(teams.keys()) > 0:
        last_five_games  = np.array(teams[team_name]).dot(weights[0:len(teams[team_name])])
      else:
        last_five_games = 0

      return {'last_five_all_games': last_five_all_games, 'last_five_games': last_five_games}

    def rename_key_by_team(self, team, h):
      print(h)
      result = {}
      for k, value in h.items():
        result[team+"_"+k] = value
      return result