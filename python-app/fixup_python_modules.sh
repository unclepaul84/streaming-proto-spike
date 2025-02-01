PATH=$PATH:../protoc/bin

protol -o ./proto --in-place --create-package --exclude-google-imports protoc --proto-path=../ ../*.proto
