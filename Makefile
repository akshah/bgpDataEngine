all:
    pip3.4 install --user -r requirements.txt

test:
    echo "No tests yet"

clean:
    rm -rf *.egg *.egg-info
    rm -rf build/ dist/
    rm -rf *.log logs/

