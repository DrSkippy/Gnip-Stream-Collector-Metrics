use stream_twitter_metrics;

DROP TABLE IF EXISTS `language`;
CREATE TABLE `language` (
      `datetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
      `language` varchar(8) COLLATE utf8_unicode_ci NOT NULL,
      `count` int(11) NOT NULL DEFAULT '0',
      PRIMARY KEY (`datetime`,`language`),
      KEY `index_on_lang` (`language`),
      KEY `index_on_time_id` (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

DROP TABLE IF EXISTS `verb`;
CREATE TABLE `verb` (
      `datetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
      `verb` varchar(12) COLLATE utf8_unicode_ci NOT NULL,
      `count` int(11) NOT NULL DEFAULT '0',
      PRIMARY KEY (`datetime`,`verb`),
      KEY `index_on_verb` (`verb`),
      KEY `index_on_time_id` (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

DROP TABLE IF EXISTS `counts`;
CREATE TABLE `counts` (
      `datetime` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
      `count` int(11) NOT NULL DEFAULT '0',
      PRIMARY KEY (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
