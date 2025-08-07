import smtplib
from email.mime.text import MIMEText
from cassandra.cluster import Cluster

class PriceAlert:
    def __init__(self):
        self.cluster = Cluster(['cassandra'])
        self.session = self.cluster.connect('market_data')
        
        # Email config (should be moved to environment variables)
        self.smtp_server = "smtp.example.com"
        self.smtp_port = 587
        self.email_from = "alerts@stock-system.com"
        self.email_password = "your_email_password"
    
    def check_alerts(self, symbol, threshold_pct=0.05):
        # Get latest two prices
        query = f"""
        SELECT price FROM stock_prices 
        WHERE symbol = '{symbol}' 
        LIMIT 2
        """
        rows = list(self.session.execute(query))
        
        if len(rows) < 2:
            return False
        
        current_price = rows[0].price
        previous_price = rows[1].price
        change_pct = abs(current_price - previous_price) / previous_price
        
        if change_pct >= threshold_pct:
            self.send_alert(symbol, current_price, previous_price, change_pct)
            return True
        
        return False
    
    def send_alert(self, symbol, current_price, previous_price, change_pct):
        subject = f"Price Alert: {symbol} changed by {change_pct:.2%}"
        body = f"""
        Stock {symbol} has significant price movement:
        - Previous Price: ${previous_price:.2f}
        - Current Price: ${current_price:.2f}
        - Change: {change_pct:.2%}
        """
        
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.email_from
        msg['To'] = "recipient@example.com"
        
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_from, self.email_password)
                server.send_message(msg)
            print(f"Alert sent for {symbol}")
        except Exception as e:
            print(f"Failed to send alert: {e}")

# Example usage:
# alert = PriceAlert()
# alert.check_alerts('AAPL', 0.05)  # 5% threshold