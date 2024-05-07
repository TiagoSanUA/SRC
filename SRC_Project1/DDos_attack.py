from vymgmt import Router
import re


def main():
    FW1_router_ip = '192.1.1.1'
    FW2_router_ip = '192.1.1.2'
    username = 'vyos'
    password = 'vyos'
    attackers = ['192.1.1.54', '192.1.1.56', '192.1.1.108']

    router = Router(FW1_router_ip, username, password)
    try:
        router.login()
        router.configure()
        for attacker in attackers:
            router.set(f'firewall group address-group BLOCK_ATK address {attacker};')
        router.commit()
        router.save()

        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        router.close()

    router = Router(FW2_router_ip, username, password)
    try:
        router.login()
        router.configure()
        for attacker in attackers:
            router.set(f'firewall group address-group BLOCK_ATK address {attacker};')
        router.commit()
        router.save()

        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        router.close()


if __name__ == "__main__":
    main()