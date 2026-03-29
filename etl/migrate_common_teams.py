import time
from sqlalchemy import text
from db import get_engine

DDL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'common' AND t.name = 'teams'
)
BEGIN
    CREATE TABLE common.teams (
        league           VARCHAR(10)   NOT NULL,
        team_id          BIGINT        NOT NULL,
        team_name        NVARCHAR(100) NOT NULL,
        tricode          VARCHAR(10)   NULL,
        conference       VARCHAR(50)   NULL,
        team_index       INT           NULL,
        sport_key        VARCHAR(50)   NULL,
        participant_id   VARCHAR(36)   NULL,
        created_at       DATETIME2     DEFAULT GETUTCDATE(),
        CONSTRAINT PK_common_teams PRIMARY KEY (league, team_id)
    );
    CREATE INDEX IX_common_teams_tricode      ON common.teams (league, tricode);
    CREATE INDEX IX_common_teams_participant  ON common.teams (participant_id);
END
"""

# (participant_id, sport_key, team_name, league, team_index, tricode, conference, team_id)
ROWS = [
    ('par_01hqmkr1xsfxmrj5pdq0f23asx','americanfootball_nfl','Arizona Cardinals','NFL',1,'ARI','NFC West',1),
    ('par_01hqmkr1xtexkbhkq7ct921rne','americanfootball_nfl','Atlanta Falcons','NFL',2,'ATL','NFC South',2),
    ('par_01hqmkr1xvev9rf557fy09k2cx','americanfootball_nfl','Baltimore Ravens','NFL',3,'BAL','AFC North',3),
    ('par_01hqmkr1xwe6prjwr3j4gpqwx8','americanfootball_nfl','Buffalo Bills','NFL',4,'BUF','AFC East',4),
    ('par_01hqmkr1xxf2ebbqzb95qzxxxm','americanfootball_nfl','Carolina Panthers','NFL',5,'CAR','NFC South',5),
    ('par_01hqmkr1xye20ahvp8fr2bvt74','americanfootball_nfl','Chicago Bears','NFL',6,'CHI','NFC North',6),
    ('par_01hqmkr1xze7xbceshy9tka512','americanfootball_nfl','Cincinnati Bengals','NFL',7,'CIN','AFC North',7),
    ('par_01hqmkr1y0ez5bem3gdncd8a0d','americanfootball_nfl','Cleveland Browns','NFL',8,'CLE','AFC North',8),
    ('par_01hqmkr1y1esas88pmaxe87by4','americanfootball_nfl','Dallas Cowboys','NFL',9,'DAL','NFC East',9),
    ('par_01hqmkr1y2e15tjsz9afcsj7da','americanfootball_nfl','Denver Broncos','NFL',10,'DEN','AFC West',10),
    ('par_01hqmkr1y3fex9sq94dgg1107y','americanfootball_nfl','Detroit Lions','NFL',11,'DET','NFC North',11),
    ('par_01hqmkr1y4ez38hyananses4hq','americanfootball_nfl','Green Bay Packers','NFL',12,'GB','NFC North',12),
    ('par_01hqmkr1y5f63reha26n71p2jx','americanfootball_nfl','Houston Texans','NFL',13,'HOU','AFC South',13),
    ('par_01hqmkr1y6f10rxbf8y2y2xthh','americanfootball_nfl','Indianapolis Colts','NFL',14,'IND','AFC South',14),
    ('par_01hqmkr1y7e2r9kcn2qe0dt1d5','americanfootball_nfl','Jacksonville Jaguars','NFL',15,'JAX','AFC South',15),
    ('par_01hqmkr1y8e9gt2q2rhmv196pv','americanfootball_nfl','Kansas City Chiefs','NFL',16,'KC','AFC West',16),
    ('par_01hqmkr1y9fkaaeekn9w035jft','americanfootball_nfl','Las Vegas Raiders','NFL',17,'LV','AFC West',23),
    ('par_01hqmkr1yafvas6wtv3jfs9f7a','americanfootball_nfl','Los Angeles Chargers','NFL',18,'LAC','AFC West',27),
    ('par_01hqmkr1ybfmfb8mhz10drfe21','americanfootball_nfl','Los Angeles Rams','NFL',19,'LAR','NFC West',26),
    ('par_01hqmkr1ycf7dsbr1997gz03y9','americanfootball_nfl','Miami Dolphins','NFL',20,'MIA','AFC East',17),
    ('par_01hqmkr1ydf6vrfmd5f07caj88','americanfootball_nfl','Minnesota Vikings','NFL',21,'MIN','NFC North',18),
    ('par_01hqmkr1yeffz9y9spwv8bx3na','americanfootball_nfl','New England Patriots','NFL',22,'NE','AFC East',19),
    ('par_01hqmkr1yfe62tp0rvy8bn2jyc','americanfootball_nfl','New Orleans Saints','NFL',23,'NO','NFC South',20),
    ('par_01hqmkr1ygfzrv5sqe2v97c43e','americanfootball_nfl','New York Giants','NFL',24,'NYG','NFC East',21),
    ('par_01hqmkr1yhe4sb3y0wfzga67tf','americanfootball_nfl','New York Jets','NFL',25,'NYJ','AFC East',22),
    ('par_01hqmkr1yjedgakx37g743855e','americanfootball_nfl','Philadelphia Eagles','NFL',26,'PHI','NFC East',24),
    ('par_01hqmkr1yker5bwcznt0b1jpj1','americanfootball_nfl','Pittsburgh Steelers','NFL',27,'PIT','AFC North',25),
    ('par_01hqmkr1ymfv0a8kfg96ha10ag','americanfootball_nfl','San Francisco 49ers','NFL',28,'SF','NFC West',28),
    ('par_01hqmkr1ynfwaa91y9zvagkavd','americanfootball_nfl','Seattle Seahawks','NFL',29,'SEA','NFC West',29),
    ('par_01hqmkr1ypeszan8sq8dh7rqbg','americanfootball_nfl','Tampa Bay Buccaneers','NFL',30,'TB','NFC South',30),
    ('par_01hqmkr1yqexebpc06vyfwxqqm','americanfootball_nfl','Tennessee Titans','NFL',31,'TEN','AFC South',31),
    ('par_01hqmkr1yrfsvbjjasn01a7xz4','americanfootball_nfl','Washington Commanders','NFL',32,'WAS','NFC East',32),
    ('par_01hqmkrdw0fn28675jydfxxfwn','baseball_mlb','Arizona Diamondbacks','MLB',33,'AZ','NL West',109),
    ('par_01hqmkrdwmeba8n6jxwetryhk7','baseball_mlb','Athletics','MLB',34,'ATH','AL West',133),
    ('par_01hqmkrdw1f3ma6f606jcfz6ax','baseball_mlb','Atlanta Braves','MLB',35,'ATL','NL East',144),
    ('par_01hqmkrdw2ey5sm4v3924da7ec','baseball_mlb','Baltimore Orioles','MLB',36,'BAL','AL East',110),
    ('par_01hqmkrdw3e9fr76tt7m3bns4m','baseball_mlb','Boston Red Sox','MLB',37,'BOS','AL East',111),
    ('par_01hqmkrdw4frbt04kwb6z2exvr','baseball_mlb','Chicago Cubs','MLB',38,'CHC','NL Central',112),
    ('par_01hqmkrdw5ezqtm9np3kavr1py','baseball_mlb','Chicago White Sox','MLB',39,'CWS','AL Central',145),
    ('par_01hqmkrdw6fekahgy3j7ndevq4','baseball_mlb','Cincinnati Reds','MLB',40,'CIN','NL Central',113),
    ('par_01hqmkrdw7e92rp1xj0h53tgtg','baseball_mlb','Cleveland Guardians','MLB',41,'CLE','AL Central',114),
    ('par_01hqmkrdw8end82x3xb2fhehv3','baseball_mlb','Colorado Rockies','MLB',42,'COL','NL West',115),
    ('par_01hqmkrdw9fspanamf83tq59ax','baseball_mlb','Detroit Tigers','MLB',43,'DET','AL Central',116),
    ('par_01hqmkrdwaebprg6k94kc3f7z5','baseball_mlb','Houston Astros','MLB',44,'HOU','AL West',117),
    ('par_01hqmkrdwbfyhbre77gx6mgyfc','baseball_mlb','Kansas City Royals','MLB',45,'KC','AL Central',118),
    ('par_01hqmkrdwce34s2bm9drh78xqe','baseball_mlb','Los Angeles Angels','MLB',46,'LAA','AL West',108),
    ('par_01hqmkrdwdevbrzmkvf1gfsn5g','baseball_mlb','Los Angeles Dodgers','MLB',47,'LAD','NL West',119),
    ('par_01hqmkrdweewjaqv7tj573a2fj','baseball_mlb','Miami Marlins','MLB',48,'MIA','NL East',146),
    ('par_01hqmkrdwfecjscs9zecbc5xhd','baseball_mlb','Milwaukee Brewers','MLB',49,'MIL','NL Central',158),
    ('par_01hqmkrdwgfpvb3reb59xtp0sa','baseball_mlb','Minnesota Twins','MLB',50,'MIN','AL Central',142),
    ('par_01hqmkrdwjf8jrcxq2twptkdy1','baseball_mlb','New York Mets','MLB',51,'NYM','NL East',121),
    ('par_01hqmkrdwke59s2wa1r1bpwvj5','baseball_mlb','New York Yankees','MLB',52,'NYY','AL East',147),
    ('par_01hqmkrdwnedzacd3r40hyrnxg','baseball_mlb','Philadelphia Phillies','MLB',53,'PHI','NL East',143),
    ('par_01hqmkrdwpfmnbebtxvpfv1a7z','baseball_mlb','Pittsburgh Pirates','MLB',54,'PIT','NL Central',134),
    ('par_01hqmkrdwqew5vtmpr3tbxa8gd','baseball_mlb','San Diego Padres','MLB',55,'SD','NL West',135),
    ('par_01hqmkrdwrex5b7e8x18zv15pg','baseball_mlb','San Francisco Giants','MLB',56,'SF','NL West',137),
    ('par_01hqmkrdwse0qawha9hvcjkjf4','baseball_mlb','Seattle Mariners','MLB',57,'SEA','AL West',136),
    ('par_01hqmkrdwtev1bxvy5y0yhvyyj','baseball_mlb','St. Louis Cardinals','MLB',58,'STL','NL Central',138),
    ('par_01hqmkrdwvfed94w1vd601r5wt','baseball_mlb','Tampa Bay Rays','MLB',59,'TB','AL East',139),
    ('par_01hqmkrdwwfvssvhjx9f6rw2r8','baseball_mlb','Texas Rangers','MLB',60,'TEX','AL West',140),
    ('par_01hqmkrdwxfhabkjbbbk4by03r','baseball_mlb','Toronto Blue Jays','MLB',61,'TOR','AL East',141),
    ('par_01hqmkrdwyfdftwzgjyp2sevr4','baseball_mlb','Washington Nationals','MLB',62,'WSH','NL East',120),
    ('par_01hqmkq6fceknv7cwebesgrx03','basketball_nba','Atlanta Hawks','NBA',63,'ATL','Southeast',1610612737),
    ('par_01hqmkq6fdf1pvq7jgdd7hdmpf','basketball_nba','Boston Celtics','NBA',64,'BOS','Atlantic',1610612738),
    ('par_01hqmkq6fefp3r8597cv3wj3cr','basketball_nba','Brooklyn Nets','NBA',65,'BKN','Atlantic',1610612751),
    ('par_01hqmkq6fffqq9gze9hqf1fwn6','basketball_nba','Charlotte Hornets','NBA',66,'CHA','Southeast',1610612766),
    ('par_01hqmkq6fgf7krk5evvjfy9mr1','basketball_nba','Chicago Bulls','NBA',67,'CHI','Central',1610612741),
    ('par_01hqmkq6fhec0t7bezwtzqv0fq','basketball_nba','Cleveland Cavaliers','NBA',68,'CLE','Central',1610612739),
    ('par_01hqmkq6fje5rrsbnbx97seg30','basketball_nba','Dallas Mavericks','NBA',69,'DAL','Southwest',1610612742),
    ('par_01hqmkq6fkf9r8wh7303b8hy40','basketball_nba','Denver Nuggets','NBA',70,'DEN','Northwest',1610612743),
    ('par_01hqmkq6fmfyjsnjtexnh7vdwm','basketball_nba','Detroit Pistons','NBA',71,'DET','Central',1610612765),
    ('par_01hqmkq6fne7nsfvf365y98r0h','basketball_nba','Golden State Warriors','NBA',72,'GSW','Pacific',1610612744),
    ('par_01hqmkq6fpetbrrcfrsgh982ed','basketball_nba','Houston Rockets','NBA',73,'HOU','Southwest',1610612745),
    ('par_01hqmkq6fqfcgtzyhmce1m5g86','basketball_nba','Indiana Pacers','NBA',74,'IND','Central',1610612754),
    ('par_01hqmkq6frex7v4gfdda5g204q','basketball_nba','Los Angeles Clippers','NBA',75,'LAC','Pacific',1610612746),
    ('par_01hqmkq6fser5vcxm0fprbrcjz','basketball_nba','Los Angeles Lakers','NBA',76,'LAL','Pacific',1610612747),
    ('par_01hqmkq6ftf24avayqxfd8840c','basketball_nba','Memphis Grizzlies','NBA',77,'MEM','Southwest',1610612763),
    ('par_01hqmkq6fvemwsm7z5cv5d24q1','basketball_nba','Miami Heat','NBA',78,'MIA','Southeast',1610612748),
    ('par_01hqmkq6fwfsdt20kfva62r4t7','basketball_nba','Milwaukee Bucks','NBA',79,'MIL','Central',1610612749),
    ('par_01hqmkq6fxfsca5pyfcqjjtaw9','basketball_nba','Minnesota Timberwolves','NBA',80,'MIN','Northwest',1610612750),
    ('par_01hqmkq6fyed09scyb5yv7xt2s','basketball_nba','New Orleans Pelicans','NBA',81,'NOP','Southwest',1610612740),
    ('par_01hqmkq6fzfvyvrsb30jj85ade','basketball_nba','New York Knicks','NBA',82,'NYK','Atlantic',1610612752),
    ('par_01hqmkq6g0f44vajb32zbpwnbr','basketball_nba','Oklahoma City Thunder','NBA',83,'OKC','Northwest',1610612760),
    ('par_01hqmkq6g1edytehr6s6fzpfzn','basketball_nba','Orlando Magic','NBA',84,'ORL','Southeast',1610612753),
    ('par_01hqmkq6g2egdt6asd59yp0vxp','basketball_nba','Philadelphia 76ers','NBA',85,'PHI','Atlantic',1610612755),
    ('par_01hqmkq6g3fnvve924dqwzxh90','basketball_nba','Phoenix Suns','NBA',86,'PHX','Pacific',1610612756),
    ('par_01hqmkq6g4fz3tkx3w1w30mxb8','basketball_nba','Portland Trail Blazers','NBA',87,'POR','Northwest',1610612757),
    ('par_01hqmkq6g5f22am45k13jkjtn2','basketball_nba','Sacramento Kings','NBA',88,'SAC','Pacific',1610612758),
    ('par_01hqmkq6g6fzr95vwm0a66qmh5','basketball_nba','San Antonio Spurs','NBA',89,'SAS','Southwest',1610612759),
    ('par_01hqmkq6g7ffc92tna8axq3t4s','basketball_nba','Toronto Raptors','NBA',90,'TOR','Atlantic',1610612761),
    ('par_01hqmkq6g8e11sj1g4zv9z9snc','basketball_nba','Utah Jazz','NBA',91,'UTA','Northwest',1610612762),
    ('par_01hqmkq6g9f0d9n781t7z01mn2','basketball_nba','Washington Wizards','NBA',92,'WAS','Southeast',1610612764),
]


def wait_for_db(engine, retries=3, wait=45):
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            print(f'DB ready on attempt {attempt}')
            return
        except Exception as e:
            print(f'Attempt {attempt} failed: {e}')
            if attempt < retries:
                time.sleep(wait)
    raise RuntimeError('DB not available after retries')


def run():
    engine = get_engine()
    wait_for_db(engine)

    with engine.begin() as conn:
        conn.execute(text(DDL))
        print('DDL executed')

        merge = text("""
            MERGE common.teams AS target
            USING (VALUES (
                :league, :team_id, :team_name, :tricode,
                :conference, :team_index, :sport_key, :participant_id
            )) AS source (
                league, team_id, team_name, tricode,
                conference, team_index, sport_key, participant_id
            )
            ON target.league = source.league AND target.team_id = source.team_id
            WHEN MATCHED THEN UPDATE SET
                team_name      = source.team_name,
                tricode        = source.tricode,
                conference     = source.conference,
                team_index     = source.team_index,
                sport_key      = source.sport_key,
                participant_id = source.participant_id
            WHEN NOT MATCHED THEN INSERT (
                league, team_id, team_name, tricode,
                conference, team_index, sport_key, participant_id
            ) VALUES (
                source.league, source.team_id, source.team_name, source.tricode,
                source.conference, source.team_index, source.sport_key, source.participant_id
            );
        """)

        for row in ROWS:
            conn.execute(merge, {
                'participant_id': row[0],
                'sport_key':      row[1],
                'team_name':      row[2],
                'league':         row[3],
                'team_index':     row[4],
                'tricode':        row[5],
                'conference':     row[6],
                'team_id':        row[7],
            })

        print(f'Upserted {len(ROWS)} rows into common.teams')


if __name__ == '__main__':
    run()
