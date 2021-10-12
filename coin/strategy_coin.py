import os
import sys
import numpy as np
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.static import *
from utility.setting import *

MONEYTOP_MINUTE = 10        # 최근거래대금순위을 집계할 시간
MONEYTOP_RANK = 15          # 최근거래대금순위중 관심종목으로 선정할 순위


class StrategyCoin:
    def __init__(self, qlist):
        """
                    0        1       2        3       4       5          6        7      8      9     10
        qlist = [windowQ, soundQ, query1Q, query2Q, teleQ, sreceivQ, creceivQ, stockQ, coinQ, sstgQ, cstgQ,
                 tick1Q, tick2Q, tick3Q, tick4Q, tick5Q]
                   11       12      13     14      15
        """
        self.windowQ = qlist[0]
        self.query2Q = qlist[3]
        self.coinQ = qlist[8]
        self.cstgQ = qlist[10]

        con = sqlite3.connect(DB_COIN_STRETEGY)
        df = pd.read_sql('SELECT * FROM init', con).set_index('index')
        if len(df) > 0 and '현재전략' in df.index:
            self.init_var = compile(df['전략코드']['현재전략'], '<string>', 'exec')
        else:
            self.init_var = None

        df = pd.read_sql('SELECT * FROM buy', con).set_index('index')
        if len(df) > 0 and '현재전략' in df.index:
            self.buystrategy = compile(df['전략코드']['현재전략'], '<string>', 'exec')
        else:
            self.buystrategy = None

        df = pd.read_sql('SELECT * FROM sell', con).set_index('index')
        con.close()
        if len(df) > 0 and '현재전략' in df.index:
            self.sellstrategy = compile(df['전략코드']['현재전략'], '<string>', 'exec')
        else:
            self.sellstrategy = None

        if self.init_var is not None:
            try:
                exec(self.init_var, None, locals())
            except Exception as e:
                self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - __init__ {e}'])

        self.list_buy = []
        self.list_sell = []
        self.int_tujagm = 0
        self.dt_mtct = None

        self.df_mt = pd.DataFrame(columns=['거래대금순위'])
        self.df_mc = pd.DataFrame(columns=['최근거래대금'])
        self.dict_cdjm = {}     # key: 종목코드, value: DataFrame
        self.dict_gsjm = {}     # key: 종목코드, value: DataFrame
        self.dict_hgjr = {}     # key: 종목코드, value: list
        self.dict_time = {
            '관심종목': now(),
            '연산시간': now(),
            '거래대금순위기록': now(),
            '거래대금순위저장': now()
        }
        self.Start()

    def Start(self):
        while True:
            data = self.cstgQ.get()
            if type(data) == int:
                self.UpdateTotaljasan(data)
            elif type(data) == list:
                if len(data) == 2:
                    self.UpdateList(data[0], data[1])
                elif len(data) == 23:
                    self.UpdateOrderbook(data)
                elif len(data) == 14:
                    self.BuyStrategy(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                                     data[8], data[9], data[10], data[11], data[12], data[13])
                elif len(data) == 5:
                    self.SellStrategy(data[0], data[1], data[2], data[3], data[4])

            if now() > self.dict_time['관심종목']:
                self.windowQ.put([ui_num['C관심종목'], self.dict_gsjm])
                self.dict_time['관심종목'] = timedelta_sec(1)

    def UpdateTotaljasan(self, data):
        self.int_tujagm = data

    def UpdateOrderbook(self, data):
        code = data[0]
        data.remove(code)
        self.dict_hgjr[code] = data

    def UpdateList(self, gubun, codes):
        if gubun in ['매수완료', '매수취소']:
            if codes in self.list_buy:
                self.list_buy.remove(codes)
        elif gubun in ['매도완료', '매도취소']:
            if codes in self.list_sell:
                self.list_sell.remove(codes)
        elif gubun == '매수전략':
            self.buystrategy = compile(codes, '<string>', 'exec')
        elif gubun == '매도전략':
            self.sellstrategy = compile(codes, '<string>', 'exec')
        elif gubun == '매수전략중지':
            self.buystrategy = None
        elif gubun == '매도전략중지':
            self.sellstrategy = None

    def BuyStrategy(self, 현재가, 시가, 고가, 저가, 등락율, 당일거래대금, 초당매수수량, 초당매도수량,
                    누적매수량, 누적매도량, 종목명, 체결시간, 수신시간, 잔고종목):
        dt = 체결시간[:13]
        if 종목명 not in self.dict_cdjm.keys():
            columns = ['10초누적거래대금', '10초전당일거래대금']
            self.dict_cdjm[종목명] = pd.DataFrame([[0, 당일거래대금]], columns=columns, index=[dt])
        elif dt != self.dict_cdjm[종목명].index[-1]:
            직전당일거래대금 = self.dict_cdjm[종목명]['10초전당일거래대금'][-1]
            self.dict_cdjm[종목명].at[dt] = 당일거래대금 - 직전당일거래대금, 당일거래대금
            if len(self.dict_cdjm[종목명]) == MONEYTOP_MINUTE * 6:
                if 등락율 > 0:
                    self.df_mc.at[종목명] = self.dict_cdjm[종목명]['10초누적거래대금'].sum()
                elif 종목명 in self.df_mc.index:
                    self.df_mc.drop(index=종목명, inplace=True)
                self.dict_cdjm[종목명].drop(index=self.dict_cdjm[종목명].index[0], inplace=True)

        if len(self.df_mc) == 0:
            return

        self.df_mc.sort_values(by=['최근거래대금'], ascending=False, inplace=True)
        if 종목명 in self.df_mc.index[:MONEYTOP_RANK] and 종목명 not in self.dict_gsjm.keys():
            data = np.zeros((DICT_SET['평균값계산틱수2'] + 2, len(columns_gj1))).tolist()
            df = pd.DataFrame(data, columns=columns_gj1)
            self.dict_gsjm[종목명] = df.copy()
        elif 종목명 not in self.df_mc.index[:MONEYTOP_RANK] and 종목명 in self.dict_gsjm.keys():
            del self.dict_gsjm[종목명]

        if now() > self.dict_time['거래대금순위기록']:
            self.UpdateMoneyTop(체결시간)
            self.dict_time['거래대금순위기록'] = timedelta_sec(1)

        if 종목명 not in self.dict_gsjm.keys():
            return

        고저평균 = (고가 + 저가) / 2
        고저평균대비등락율 = round((현재가 / 고저평균 - 1) * 100, 2)
        직전당일거래대금 = self.dict_gsjm[종목명]['당일거래대금'][0]
        초당거래대금 = 0 if 직전당일거래대금 == 0 else int(당일거래대금 - 직전당일거래대금)

        try:
            체결강도 = round(누적매수량 / 누적매도량 * 100, 2)
        except ZeroDivisionError:
            체결강도 = 500.
        if 체결강도 > 500:
            체결강도 = 500.

        self.dict_gsjm[종목명] = self.dict_gsjm[종목명].shift(1)
        self.dict_gsjm[종목명].at[0] = 등락율, 고저평균대비등락율, 초당거래대금, 당일거래대금, 체결강도, 0.
        if self.dict_gsjm[종목명]['체결강도'][DICT_SET['평균값계산틱수2']] != 0.:
            평균값인덱스 = DICT_SET['평균값계산틱수2'] + 1
            초당거래대금평균 = int(self.dict_gsjm[종목명]['초당거래대금'][1:평균값인덱스].mean())
            체결강도평균 = round(self.dict_gsjm[종목명]['체결강도'][1:평균값인덱스].mean(), 2)
            최고체결강도 = round(self.dict_gsjm[종목명]['체결강도'][1:평균값인덱스].max(), 2)
            self.dict_gsjm[종목명].at[평균값인덱스] = 0., 0., 초당거래대금평균, 0, 체결강도평균, 최고체결강도

            if 잔고종목:
                return
            if 종목명 in self.list_buy:
                return

            매수 = True
            매도총잔량, 매수총잔량, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, \
                매수호가4, 매수호가5, 매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3, \
                매수잔량4, 매수잔량5 = self.dict_hgjr[종목명]

            if self.buystrategy is not None:
                try:
                    exec(self.buystrategy, None, locals())
                except Exception as e:
                    self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - BuyStrategy {e}'])

        if now() > self.dict_time['연산시간']:
            gap = (now() - 수신시간).total_seconds()
            self.windowQ.put([ui_num['C단순텍스트'], f'전략스 연산 시간 알림 - 수신시간과 연산시간의 차이는 [{gap}]초입니다.'])
            self.dict_time['연산시간'] = timedelta_sec(60)

    def SellStrategy(self, 종목명, 수익률, 보유수량, 현재가, 매수시간):
        if 종목명 not in self.dict_gsjm.keys() or 종목명 not in self.dict_hgjr.keys():
            return
        if 종목명 in self.list_sell:
            return
        if self.dict_gsjm[종목명]['체결강도'][DICT_SET['평균값계산틱수1']] == 0.:
            return

        매도 = False
        평균값인덱스 = DICT_SET['평균값계산틱수2'] + 1
        등락율 = self.dict_gsjm[종목명]['등락율'][0]
        고저평균대비등락율 = self.dict_gsjm[종목명]['고저평균대비등락율'][0]
        체결강도 = self.dict_gsjm[종목명]['체결강도'][0]
        체결강도평균 = self.dict_gsjm[종목명]['체결강도'][평균값인덱스]
        최고체결강도 = self.dict_gsjm[종목명]['체결강도'][평균값인덱스]
        초당거래대금 = self.dict_gsjm[종목명]['초당거래대금'][0]
        초당거래대금평균 = self.dict_gsjm[종목명]['초당거래대금'][평균값인덱스]
        매도총잔량, 매수총잔량, 매도호가5, 매도호가4, 매도호가3, 매도호가2, 매도호가1, 매수호가1, 매수호가2, 매수호가3, 매수호가4, \
            매수호가5, 매도잔량5, 매도잔량4, 매도잔량3, 매도잔량2, 매도잔량1, 매수잔량1, 매수잔량2, 매수잔량3, 매수잔량4, \
            매수잔량5 = self.dict_hgjr[종목명]

        if self.sellstrategy is not None:
            try:
                exec(self.sellstrategy, None, locals())
            except Exception as e:
                self.windowQ.put([ui_num['C단순텍스트'], f'전략스 설정 오류 알림 - SellStrategy {e}'])

    def UpdateMoneyTop(self, dt):
        timetype = '%Y%m%d%H%M%S'
        list_text = ';'.join(list(self.df_mc.index[:MONEYTOP_RANK]))
        curr_time = dt
        curr_datetime = strp_time(timetype, curr_time)
        if self.dt_mtct is not None:
            gap_seconds = (curr_datetime - self.dt_mtct).total_seconds()
            while gap_seconds > 2:
                gap_seconds -= 1
                pre_time = strf_time(timetype, timedelta_sec(-gap_seconds, curr_datetime))
                self.df_mt.at[pre_time] = list_text
        self.df_mt.at[curr_time] = list_text
        self.dt_mtct = curr_datetime

        if now() > self.dict_time['거래대금순위저장']:
            self.query2Q.put([2, self.df_mt, 'moneytop', 'append'])
            self.df_mt = pd.DataFrame(columns=['거래대금순위'])
            self.dict_time['거래대금순위저장'] = timedelta_sec(10)
