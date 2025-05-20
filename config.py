from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=['settings.toml', '.secrets.toml'],
    validators=[
        Validator("email", must_exist=True, cast=str, condition=lambda x: isinstance(x, str) and x.count("@") == 1),
        Validator("password", must_exist=True, cast=str, len_min=8),
        Validator("browser_user_data", must_exist=True, cast=str),
        Validator("headless", cast=bool, default=True),
        Validator("search_keywords", must_exist=True, cast=list, len_min=1),
        Validator("unwanted_title_keywords", cast=list, default=[]),
        Validator("save_filename", cast=str, default="bookmeter_books"),
        Validator("skip_existing", cast=bool, default=False),
        Validator("max_workers", cast=int, gt=0, default=5),
        Validator("max_search_pages", cast=int, gt=0, default=15),
        Validator("amazon.enable", cast=bool, default=False),
        Validator("amazon.max_review_pages", cast=int, gt=0, default=3),
        Validator("retry.retry_queue_size", cast=int, gt=0, default=512),
        Validator("retry.max_retry_count", cast=int, gt=0, default=3),
        Validator("retry.backoff_factor", cast=int, gt=0, default=1),
    ]
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
