import pandas as pd
import numpy as np
import os


class Report:
    def __init__(self, df: pd.DataFrame, ticker: str = None, title: str = None) -> None:
        self.df = df
        self.ticker = "Unknown" if ticker is None else ticker
        self.title = "Unknown Strategy" if title is None else title

    def drawdown(self, equity_column):
        maxvalue = self.df[equity_column].expanding(0).max()
        drawdown = pd.Series(self.df[equity_column] - maxvalue, index=self.df[equity_column].index)
        return drawdown

    def max_drawdown(self, equity_column):
        return round(self.drawdown(equity_column).min(), 2)

    def avg_drawdown(self, equity_column):
        dd = self.drawdown(equity_column)
        return round(dd[dd < 0].mean(), 2)

    def avg_loss(self, operation_column):
        operations = self.df[operation_column].dropna()
        return round(operations[operations < 0].mean(), 2)

    def max_loss(self, operation_column):
        operations = self.df[operation_column].dropna()
        return operations.idxmin(), round(operations.min(), 2)

    def avg_gain(self, operation_column):
        operations = self.df[operation_column].dropna()
        return round(operations[operations > 0].mean(), 2)

    def max_gain(self, operation_column):
        operations = self.df[operation_column].dropna()
        return operations.idxmax(), round(operations.max(), 2)

    def gross_profit(self, operation_column):
        operations = self.df[operation_column].dropna()
        return round(operations[operations > 0].sum(), 2)

    def gross_loss(self, operation_column):
        operations = self.df[operation_column].dropna()
        return round(operations[operations <= 0].sum(), 2)

    def profit_factor(self, operation_column):
        profit = self.gross_profit(operation_column)
        loss = self.gross_loss(operation_column)
        if loss != 0:
            return round(abs(profit / loss), 2)
        return np.inf

    def percent_win(self, operation_column):
        operations = self.df[operation_column].dropna()
        return round((operations[operations > 0].count() / operations.count()) * 100, 2)

    def reward_risk_ratio(self, operation_column):
        operations = self.df[operation_column].dropna()
        if operations[operations < 0].mean() != 0:
            return round(abs(operations[operations > 0].mean() / operations[operations <= 0].mean()), 2)
        return np.inf

    def profit(self, equity_column):
        return round(self.df[equity_column][-1], 2)

    def number_of_operations(self, operation_column):
        return self.df[operation_column].dropna().count()

    def operation_stats(self, operation_column):
        operations = self.df[operation_column].dropna()
        return {'mean': round(operations.mean(), 2), 'std': round(operations.std(), 2)}

    def avg_trade(self, operation_column):
        return self.df[operation_column].dropna().mean().__round__(2)

    def delay_between_peaks(self, equity_column):
        work_df = pd.DataFrame(self.df[equity_column], index=self.df[equity_column].index)
        work_df['drawdown'] = self.drawdown(equity_column)
        work_df['delay_element'] = work_df['drawdown'].apply(
            lambda x: 1 if x < 0 else 0)
        work_df['resets'] = np.where(work_df['drawdown'] == 0, 1, 0)
        work_df['cumsum'] = work_df['resets'].cumsum()
        return pd.Series(work_df['delay_element'].groupby(work_df['cumsum']).cumsum())

    def max_delay_between_peaks(self, equity_column):
        return self.delay_between_peaks(equity_column).max()

    def avg_delay_between_peaks(self, equity_column):
        work_df = pd.DataFrame(self.df[equity_column], index=self.df[equity_column].index)
        work_df['drawdown'] = self.drawdown(equity_column)
        work_df['delay_element'] = work_df['drawdown'].apply(
            lambda x: 1 if x < 0 else np.nan)
        work_df['resets'] = np.where(work_df['drawdown'] == 0, 1, 0)
        work_df['cumsum'] = work_df['resets'].cumsum()
        work_df.dropna(inplace=True)
        a = work_df['delay_element'].groupby(work_df['cumsum']).cumsum()
        return round(a.mean(), 2)

    def calculate_report(self):
        return {
            "Profit": {
                "Profit": self.profit("open_equity"),
                "Profit Factor": self.profit_factor("operations"),
                "Gross Profit": self.gross_profit("operations"),
                "Gross Loss": self.gross_loss("operations")
            },
            "Trades": {
                "Operations": self.number_of_operations("operations"),
                "Average Trade": self.avg_trade("operations"),
                "% Winning Trades": self.percent_win("operations"),
                "% Losing Trades": 100 - self.percent_win("operations"),
                "Reward Risk Ratio": self.reward_risk_ratio("operations")
            },
            "Gains": {
                "Max": self.max_gain("operations")[1],
                "Max Date": self.max_gain("operations")[0],
                "Average": self.avg_gain("operations")
            },
            "Losses": {
                "Max": self.max_loss("operations")[1],
                "Max Date": self.max_loss("operations")[0],
                "Average": self.avg_loss("operations")
            },
            "Drawdown": {
                "Average Open": self.avg_drawdown("open_equity"),
                "Max Open": self.max_drawdown("open_equity"),
                "Average Close": self.avg_drawdown("closed_equity"),
                "Max Close": self.avg_drawdown("closed_equity")
            },
            "Peaks": {
                "Average Delay": self.avg_delay_between_peaks("open_equity"),
                "Max Delay": self.max_delay_between_peaks("open_equity")
            }
        }

    def table_report(self):
        return pd.json_normalize(self.calculate_report(), sep='.')

    def build_report(self):
        return f"""Performance Report for {self.ticker} - {self.title}

Profit:                   , {self.profit("open_equity")}
Operations:              , {self.number_of_operations("operations")}
Average Trade:           , {self.avg_trade("operations")}

Profit Factor:            , {self.profit_factor("operations")}
Gross Profit:             , {self.gross_profit("operations")}
Gross Loss:              , {self.gross_loss("operations")}

Percent Winning Trades:  , {self.percent_win("operations")} %
Percent Losing Trades:   , {100 - self.percent_win("operations")} %
Reward Risk Ratio:       , {self.reward_risk_ratio("operations")}

Max Gain:                , {self.max_gain("operations")[1]},  in date , {self.max_gain("operations")[0]}
Average Gain:            , {self.avg_gain("operations")}
Max Loss:                , {self.max_loss("operations")[1]},  in date , {self.max_loss("operations")[0]}
Average Loss:            , {self.avg_loss("operations")}

Avg Open Draw Down:      , {self.avg_drawdown("open_equity")}
Max Open Draw Down:      , {self.max_drawdown("open_equity")}

Avg Closed Draw Down:    , {self.avg_drawdown("closed_equity")}
Max Closed Draw Down:    , {self.max_drawdown("closed_equity")}

Avg Delay Between Peaks: , {self.avg_delay_between_peaks("open_equity")}
Max Delay Between Peaks: , {self.max_delay_between_peaks("open_equity")}

        """

    def write_report(self):
        text_output = self.build_report()

        output_path = os.path.join(*[".", "resources", "reports", self.ticker + '.txt'])

        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))

        with open(output_path, 'a') as f:
            print(text_output, file=f)
