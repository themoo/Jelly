import jel

def main():
    r = jel.Runner()
    r.start('tcp://*:5556','tcp://*:5557');

if __name__ == "__main__":
    main()