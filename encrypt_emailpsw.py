if __name__ == '__main__':
    import sys
    import os
    import base64
    if len(sys.argv)>1:
        print(base64.b64encode(sys.argv[1].encode())[::-1])
        os.system("pause")
        sys.exit()

