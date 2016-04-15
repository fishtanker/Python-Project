#sample Python Code for Hampton Creek
#Table of Content
#1) Line 7 ~ 35
#    --- To acquire stock fundamental data in HTML format from YHOO
#2) Line 39 ~ 158
#    --- Sample Blackjack program

import urllib.request
import os
import time

path = "C:\Python\Data\intraQuarter"

def Check_Yahoo():
    statspath = path+"/_KeyStats"
    stock_list = [x[0] for x in os.walk(statspath)]

    for stock in stock_list[1:]:
        try:
            #To create a stock ticker from the path
            stock = stock.replace("C:\Python\Data\intraQuarter/_KeyStats\\","")
            #To recreate the link by replacing the ticker variable
            link = "http://finance.yahoo.com/q/ks?s="+stock.upper()+"+Key+Statistics"
            #To read the web info
            resp = urllib.request.urlopen(link).read()
            #To save the files to the location that we wanted
            save = "C:\Python\Application\Data/"+str(stock)+".html"
            #To write the file i.e if the file is not there, it will be created.
            store = open(save,"w")
            store.write(str(resp))
            store.close()

        except Exception as e:
            print(str(e))

Check_Yahoo()


#quick black jack program that I wrote
import itertools
import random


##To build a deck of card and return a card & card value in a tuple.
def drawcard ():
    suit = ('Spade','Heart','Diamond','Club')
    number = ('A','2','3','4','5','6','7','8','9','10','J','Q','K')
    combo = list(itertools.product(suit,number)) #To cross multi two list
    
    CountMap = {('Spade', 'A'):1, ('Spade', '2'):2, ('Spade', '3'):3, ('Spade', '4'):4, ('Spade', '5'):5, ('Spade', '6'):6, ('Spade', '7'):7, ('Spade', '8'):8, ('Spade', '9'):9, ('Spade', '10'):10, ('Spade', 'J'):10, ('Spade', 'Q'):10, ('Spade', 'K'):10,
                ('Heart', 'A'):1, ('Heart', '2'):2, ('Heart', '3'):3, ('Heart', '4'):4, ('Heart', '5'):5, ('Heart', '6'):6, ('Heart', '7'):7, ('Heart', '8'):8, ('Heart', '9'):9, ('Heart', '10'):10, ('Heart', 'J'):10, ('Heart', 'Q'):10, ('Heart', 'K'):10, 
                ('Diamond', 'A'):1, ('Diamond', '2'):2, ('Diamond', '3'):3, ('Diamond', '4'):4, ('Diamond', '5'):5, ('Diamond', '6'):6, ('Diamond', '7'):7, ('Diamond', '8'):8, ('Diamond', '9'):9, ('Diamond', '10'):10, ('Diamond', 'J'):10, ('Diamond', 'Q'):10, ('Diamond', 'K'):10, 
                ('Club', 'A'):1, ('Club', '2'):2, ('Club', '3'):3, ('Club', '4'):4, ('Club', '5'):5, ('Club', '6'):6, ('Club', '7'):7, ('Club', '8'):8, ('Club', '9'):9, ('Club', '10'):10, ('Club', 'J'):10, ('Club', 'Q'):10, ('Club', 'K'):10}
    
    numval = random.choice (combo)
    return numval, CountMap[numval] #Return the suit&num value, real numeric value
  
#Senarios
#1) Player's count is greater than 21, LOSE
#2) Player's count is less than 21, but less than the dealer's count, LOSE
#3) player's count is less than 21, but greater than the dealer's count, WIN
#4) player has the same count as the dealer, TIE

def Acenum ():
    card1 = drawcard ()
    card2 = drawcard ()
    if card1[0][0] == 'A':
        newnum = int(input('You want A to remain as 1 or change it to 11? \n 1 or 11'))
        return newnum + card2[1],card1,card2
    if card2[0][0] == 'A':
        newnum = int(input('You want A to remain as 1 or change it to 11? \n 1 or 11'))
        return newnum + card1[1],card1,card2
    else:
        return card1[1] + card2[1],card1,card2
  
def result (Dealercount,count, Bet, PlayerCash):
    if count > 21:  
        return 'You lost because you are busted', PlayerCash - Bet
    elif (count < Dealercount) and Dealercount < 21: #player's is less than 21 but less than the dealer
        return 'You lost, you have a smaller hand than the deals hand', PlayerCash - Bet
    elif (count > Dealercount) and Dealercount < 21: #player's is less than 21 but less than the dealer
        return 'You won, you have a bigger hand than the deals hand', PlayerCash + Bet
    elif (count < Dealercount) and Dealercount > 21:
        return 'You won, Dealer is busted', PlayerCash + Bet
    elif (count < Dealercount) and Dealercount <= 21:
        return 'You lost, Dealer has a bigger hand', PlayerCash - Bet
    elif (count == Dealercount):
        return 'Tie result, no winner', PlayerCash
    else:
        return 'Congratulations, you won!', PlayerCash + Bet

onemore = 'Y'
PlayerBankRoll = 100
  
while onemore.upper() == 'Y':    
#Dealer Count
    Dealercard1 = drawcard ()
    Dealercard2 = drawcard ()
    Dealercount = Dealercard1[1] + Dealercard2[1]
    print ('Dealer face card is ', Dealercard1[0])


    while Dealercount < 21:
        DealerAdditional = drawcard()
        Dealercount = Dealercount + DealerAdditional[1]
        
        if Dealercount < 17:
            pass
        elif Dealercount > 17 and Dealercount < 21:
            break
        else :
            break
        
       
    #Player count
    print ('\n')
    wholecard = Acenum ()
    count = wholecard[0]
    BetAmt = int(input ('How much do you want to bet? Min:1, Max:100 \n Amount: '))
    print ('Your currently have', PlayerBankRoll, 'dollars on hand')
    print ('You want to bet ', BetAmt, 'dollars')
    print ('\n')
    print ('your first card is ', wholecard[1])
    print ('your second card is ', wholecard[2])
    print ('your total count is ', count)
    
    while count < 21:
        playerRes = input ('Do you want another card? Y/N \nYour selection:')
        if  playerRes.upper() == 'Y':
            newcard = drawcard()
            print ('Your next card is', newcard[0])
            print ('The old count is', count)
            count = count + newcard[1]
            print ('Your new total count is', count)
            
        else:
            break
        
    #Compare the result  
        
    result (Dealercount,count,BetAmt,PlayerBankRoll)
    cardresult = result (Dealercount,count,BetAmt,PlayerBankRoll)[0]
    PlayerBankRoll = result (Dealercount,count,BetAmt,PlayerBankRoll)[1]
    print (cardresult)
    print ('Now you have amount ',PlayerBankRoll)

    #PlayerBankRoll = cashcount ()
        
    print ('The player final count is', count)
    print ('The dealer final count is', Dealercount)
    
    if PlayerBankRoll > 0:
        onemore = input ('Do you want to play one more game? Y/N\nYour selection:')
    else:
        break
    
    
    pass
