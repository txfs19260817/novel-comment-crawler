from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=['settings.toml', '.secrets.toml'],
    validators=[
        Validator("email", must_exist=True, cast=str, condition=lambda x: isinstance(x, str) and x.count("@") == 1),
        Validator("password", must_exist=True, cast=str, len_min=8),
        Validator("browser_user_data", must_exist=True, cast=str),
        Validator("search_keywords", must_exist=True, cast=list, len_min=1),
        Validator("save_filename", cast=str, default="bookmeter_books"),
        Validator("max_workers", cast=int, gt=0, default=5),
        Validator("max_search_pages", cast=int, gt=0, default=15),
    ]
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
