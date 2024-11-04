import polars as pl


if __name__ == '__main__':
    df = pl.read_csv("stateSinglesStatReport.csv", infer_schema_length=1000000)

    # there are 5 recording errors need to be fixed first
    df = (df
          .with_columns(server=pl.when(((pl.col("matchId").is_in(["65c1321a3a00009c44c13373", "625a47e0400000e364c4e2f5",
                                           "621cedd74300005c72219d83", "62244fac450000901f833349", "5cb0a1551900000c64ffc6ff"]))
                                        & (pl.col("pointNum") == 10000)) | ((pl.col("matchId")=='621cedd74300005c72219d83') & (pl.col("pointNum") == 10001)))
                                 .then(pl.lit(1))
                                 .otherwise(pl.col("server")),
                        returner=pl.when(((pl.col("matchId").is_in(["65c1321a3a00009c44c13373", "625a47e0400000e364c4e2f5",
                                           "621cedd74300005c72219d83", "62244fac450000901f833349", "5cb0a1551900000c64ffc6ff"]))
                                         & (pl.col("pointNum") == 10000)) | ((pl.col("matchId")=='621cedd74300005c72219d83') & (pl.col("pointNum") == 10001)))
                                   .then(pl.lit(0))
                                   .otherwise(pl.col("returner")),
                        player=pl.col("player").str.to_titlecase().replace(r"Gina Dittman", "Gina Dittmann"))
          )
    # total points won
    total_points_won = (df
                        .filter(pl.col("pointWonBy") == 0)
                        .group_by("matchId", "player", "set", "date").len(name="total points won")
                        )
    # winner & ace & forced error breakdown by shotType
    win_ace_fe_breakdown = (df
                            .filter((pl.col("pointWonBy") == 0) & (pl.col("outcome").is_in(["Winner", "ForcedError", "Ace"])))
                            .group_by("matchId", "player", "outcome", "set", "shotType").len(name="count")
                            .pivot(["outcome", "shotType"], index=["matchId", "player", "set"], values="count")
                            .fill_null(0)
                            .rename({'{"ForcedError","Backhand"}': 'ForcedError-Backhand', '{"Winner","Volley"}': 'Winner-Volley',
                                     '{"Winner","Forehand"}': 'Winner-Forehand', '{"ForcedError","Forehand"}': 'ForcedError-Forehand',
                                     '{"ForcedError","Volley"}': 'ForcedError-Volley', '{"Winner","Backhand"}': 'Winner-Backhand',
                                     'null': 'Ace'})
                            .with_columns(ForcedError=pl.sum_horizontal("ForcedError-Backhand", "ForcedError-Forehand", "ForcedError-Volley"))
                            .with_columns(Winner=pl.sum_horizontal("Winner-Backhand", "Winner-Forehand", "Winner-Volley"))
                            )
    # unforced error & fault breakdown by shotType
    uf_df_breakdown = (df
                       .filter((pl.col("pointWonBy") == 1) & (pl.col("outcome").is_in(["UnforcedError", "Fault"])))
                       .group_by("matchId", "player", "shotType", "set").len(name="count")
                       .pivot("shotType", index=["matchId", "player", "set"], values="count")
                       .fill_null(0)
                       .rename({'Forehand': 'UnforcedError-Forehand', 'Backhand': 'UnforcedError-Backhand',
                                'Volley': 'UnforcedError-Volley', 'null': 'Fault'})
                       .with_columns(UnforcedError=pl.sum_horizontal("UnforcedError-Backhand", "UnforcedError-Forehand", "UnforcedError-Volley"))
                       )
    # break point -> breakPoint=True server=1 pointWonBy=0
    break_point = (df
                   .filter(pl.col("breakPoint") & (pl.col("server") == 1))
                   .group_by("matchId", "player", "set", "pointWonBy").len(name="count")
                   .pivot("pointWonBy", index=["matchId", "player", "set"], values="count")
                   .fill_null(0)
                   .with_columns(totalBreakPoint=pl.sum_horizontal("0", "1"))
                   .rename({'0': 'breakPoint'})
                   .drop('1')
                   )
    # first serve
    first_serve = (df
                   .filter(pl.col("server") == 0)
                   .group_by("matchId", "player", "set", "firstServeIn", "pointWonBy").len(name="count")
                   .pivot(["firstServeIn", "pointWonBy"], index=["matchId", "player", "set"], values="count")
                   .fill_null(0)
                   .with_columns(totalFirstServe=pl.sum_horizontal("{true,0}", "{true,1}", "{false,0}", "{false,1}"),
                                 firstServeIn=pl.sum_horizontal("{true,0}", "{true,1}"),
                                 totalSecondServe=pl.sum_horizontal("{false,0}", "{false,1}"))
                   .rename({'{true,0}': 'firstServeWon'})
                   .drop("{true,1}", "{false,0}", "{false,1}")
                   )
    # second serve
    second_serve = (df
                    .filter((pl.col("server") == 0) & (~pl.col("firstServeIn")) & (pl.col("outcome") != "Fault"))
                    .group_by("matchId", "player", "set", "pointWonBy").len(name="count")
                    .pivot("pointWonBy", index=["matchId", "player", "set"], values="count")
                    .fill_null(0)
                    .with_columns(secondServeIn=pl.sum_horizontal("0", "1"))
                    .rename({'0': 'secondServeWon'})
                    .drop("1")
                    )
    # returns
    returns = (df
               .filter((pl.col("server") == 1) & (pl.col("outcome") != "Fault"))
               .group_by("matchId", "player", "set", "firstServeIn", "returnInPlay").len(name="count")
               .pivot(["firstServeIn", "returnInPlay"], index=["matchId", "player", "set"], values="count")
               .fill_null(0)
               .with_columns(firstReturnTotal=pl.sum_horizontal("{true,true}", "{true,false}"),
                             secondReturnTotal=pl.sum_horizontal("{false,true}", "{false,false}"),)
               .rename({'{true,true}': 'firstReturn', '{false,true}': 'secondReturn'})
               .drop("{false,false}", "{true,false}")
               )
    # rallies
    rally = (df
             .with_columns(rallyBin=pl.col("rallyLength").cut([4, 8], labels=["short", "med", "long"]))
             .group_by("matchId", "player", "set", "rallyBin", "pointWonBy").len(name="count")
             .pivot(["rallyBin", "pointWonBy"], index=["matchId", "player", "set"], values="count")
             .fill_null(0)
             .with_columns(shortRallyTotal=pl.sum_horizontal('{"short",0}', '{"short",1}'),
                           medRallyTotal=pl.sum_horizontal('{"med",0}', '{"med",1}'),
                           longRallyTotal=pl.sum_horizontal('{"long",0}', '{"long",1}'),
                           )
             .rename({'{"short",0}': 'shortRallyWon', '{"med",0}': 'medRallyWon', '{"long",0}': 'longRallyWon'})
             .drop('{"short",1}', '{"med",1}', '{"long",1}')
             )
    # service games
    service_game = (df
                    .filter((~pl.col("tiebreaker")) & (pl.col("server") == 0))
                    .group_by("matchId", "player", "set")
                    .agg(serviceGameTotal=pl.struct("game").n_unique())
                    )
    service_game_won = (df
                        .filter((~pl.col("tiebreaker")) & (pl.col("server") == 0) & (pl.col("gameWonBy") == 0))
                        .group_by("matchId", "player", "set")
                        .agg(serviceGameWon=pl.struct("game").n_unique())
                        )
    # set won flag
    set_won_flag = (df
                    .group_by("matchId", "player", "set", "finalScore", "tiebreaker").agg()
                    .with_columns(pl.col("finalScore").str.split(by="|").alias("split"))
                    .with_columns(pl.when((pl.col("set") != 3) &
                                          (pl.col("split").list.get(pl.col("set")-1).str.split(by="-").list.get(0).is_in(['6', '7'])) &
                                          (pl.col("split").list.get(pl.col("set")-1).str.split(by="-").list.get(0) >
                                           pl.col("split").list.get(pl.col("set")-1).str.split(by="-").list.get(1)))
                                    .then(pl.lit('Y'))
                                    .when((pl.col("set") != 3) &
                                          (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1).is_in(['6', '7'])) &
                                          (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0) <
                                           pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1)))
                                    .then(pl.lit('N'))
                                    .when((pl.col("set") == 3) &
                                          ((pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0).is_in(['6', '7'])) &
                                          (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0) >
                                           pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1))) |
                                          (pl.col("tiebreaker") & (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0) >
                                           pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1))))
                                    .then(pl.lit('Y'))
                                    .when((pl.col("set") == 3) &
                                          ((pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1).is_in(['6', '7'])) &
                                          (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0) <
                                           pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1))) |
                                          (pl.col("tiebreaker") & (pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(0) <
                                           pl.col("split").list.get(pl.col("set") - 1).str.split(by="-").list.get(1))))
                                    .then(pl.lit('N'))
                                    .otherwise(pl.lit('incomplete'))
                                    .alias("setWonFlag"))
                    .group_by("matchId", "player", "set", "setWonFlag").agg()
                    )

    # join all the stats
    stats = (total_points_won
             .join(win_ace_fe_breakdown, on=["matchId", "player", "set"])
             .join(uf_df_breakdown, on=["matchId", "player", "set"])
             .join(break_point, on=["matchId", "player", "set"])
             .join(first_serve, on=["matchId", "player", "set"])
             .join(second_serve, on=["matchId", "player", "set"])
             .join(returns, on=["matchId", "player", "set"])
             .join(rally, on=["matchId", "player", "set"])
             .join(service_game, on=["matchId", "player", "set"])
             .join(service_game_won, on=["matchId", "player", "set"])
             .join(set_won_flag, on=["matchId", "player", "set"])
             .with_columns(pl.col(pl.UInt32).cast(pl.Int32))
             .with_columns(aggErrorMargin=(pl.col("Ace") + pl.col("Winner") + pl.col("ForcedError")) -
                                          (pl.col("Fault") + pl.col("UnforcedError")))
             )

    stats.write_csv("output.csv")

