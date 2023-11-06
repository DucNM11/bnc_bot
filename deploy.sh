sudo apt-get upgrade -y
sudo apt-get update

sudo apt-get install -y python3-pip
pip install -r requirements.txt

sudo timedatectl set-timezone UTC
crontab -l > mycron
echo "# Bot does his stuff" >> mycron
echo "0 0 * * * cd /home/pi/bot/src/ && python main.py" >> mycron
echo "0 8 * * * cd /home/pi/bot/src/ && python main.py" >> mycron
echo "0 16 * * * cd /home/pi/bot/src/ && python main.py" >> mycron

crontab mycron
rm mycron
sudo service cron reload
