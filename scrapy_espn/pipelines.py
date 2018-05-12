# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs

class SavePipeline(object):
    def open_spider(self, spider):
        self.file = codecs.open('dataset/dataset.jl', 'a', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n"
        self.file.write(line)
        return item

class CleanDataPipeline(object):
    # Remove \t\r\n
    def process_item(self, item, spider):
        for field in ['championship']:
          item[field] = item[field].replace('\t', ' ')
          item[field] = item[field].replace('\r', ' ')
          item[field] = item[field].replace('\n', ' ')
          item[field] = item[field].strip()
        
          item['home_possesion'] = str(item['home_possesion']).replace('%', ' ')
          item['away_possesion'] = str(item['away_possesion']).replace('%', ' ')

        # Parse float
        for field in ['foulsCommitted', 'last_five_all_games', 
                    'last_five_games', 'offsides', 'possesion', 'redCards',
                    'saves', 'score', 'shots', 'shots_goal', 'wonCorners', 'yellowCards']:
          for k in ['home', 'away']:
            item[k+"_"+field] = float(item[k+"_"+field])

        return item