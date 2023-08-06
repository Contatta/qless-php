.PHONY: deps
deps:
	composer install

.PHONY: test
test:
	php vendor/bin/phpunit
