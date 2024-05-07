package main

// PlayerStruct holds the player data
type PlayerStruct struct {
	Tag                     string `json:"tag"`
	Name                    string `json:"name"`
	ExpLevel                int    `json:"expLevel"`
	Trophies                int    `json:"trophies"`
	AttackWins              int    `json:"attackWins"`
	BestTrophies            int    `json:"bestTrophies"`
	Donations               int    `json:"donations"`
	DonationsReceived       int    `json:"donationsReceived"`
	BuilderHallLevel        int    `json:"builderHallLevel"`
	BuilderBaseTrophies     int    `json:"builderBaseTrophies"`
	BestBuilderBaseTrophies int    `json:"bestBuilderBaseTrophies"`
	WarStars                int    `json:"warStars"`
}
