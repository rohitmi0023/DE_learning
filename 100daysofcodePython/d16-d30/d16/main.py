from menu import MenuItem, Menu
from coffee_maker import CoffeeMaker
from money_machine import MoneyMachine

sc_menu = Menu()

sc_coffee_maker = CoffeeMaker()

money = MoneyMachine()

is_on = True

while is_on:    
    sc_list_items = sc_menu.get_items()
    choice = str(input(f'What would you like, Sir? ({sc_list_items})')).lower()   
    if choice == 'off':
        is_on = False
    elif choice == 'report':
        sc_coffee_maker.report()
        money.report()
    else:
        drink = sc_menu.find_drink(choice)        
        if sc_coffee_maker.is_resource_sufficient(drink) and money.make_payment(drink.cost):
            sc_coffee_maker.make_coffee(drink)
            
    

